#!/usr/bin/env python3
"""
pipeline.py — Shein -> garment-reconstructor
------------------------------------------------
Une os dois projetos desta pasta num fluxo só:

  1. Recebe um link de produto da Shein.
  2. Usa o `sehin-scrapper` (scraper3.py) pra abrir o link, extrair o ID/SKU/
     nome do produto e baixar a foto principal.
  3. Passa essa foto pro `garment-reconstructor` (reconstruct.py), que
     segmenta a peça e gera a versão "produto de e-commerce" (fundo branco,
     efeito manequim invisível).
  4. Copia o resultado final pra `output/<product_id>/` nesta pasta, junto
     com um `result.json` contendo o ID da peça e o caminho da foto tratada.

Cada projeto roda com o Python do seu próprio venv (as dependências não se
misturam). Nada é reescrito dentro de sehin-scrapper/ ou garment-reconstructor/
além do que esses projetos já fazem sozinhos.

Uso via CLI:
    python3 pipeline.py "<link-do-produto-shein>" [--peca short] [--steps 6]
        [--seed 123] [--quantize 8] [--low-ram]

Uso como biblioteca (ex: server.py):
    from pipeline import process, PipelineError
    result = process(url, peca="vestido", on_stage=print)

Requisitos (nada disso é instalado por este script):
    - venv de sehin-scrapper já configurado (playwright/seleniumbase etc.)
    - venv de garment-reconstructor já configurado (torch/transformers etc.)
    - venv de pinterest-inspo já configurado (requests/pillow/playwright)
    - `mflux` instalado globalmente (uv tool install --upgrade mflux) e
      `~/.local/bin` acessível (este script já garante isso no PATH do
      subprocesso, mesmo que o shell atual não tenha).
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, List, Optional

ROOT = Path(__file__).parent.resolve()
SCRAPER_DIR = ROOT / "sehin-scrapper"
GARMENT_DIR = ROOT / "garment-reconstructor"
PINTEREST_DIR = ROOT / "pinterest-inspo"
FINAL_OUTPUT_DIR = ROOT / "output"

SCRAPER_PYTHON = SCRAPER_DIR / "venv" / "bin" / "python3.12"
GARMENT_PYTHON = GARMENT_DIR / "venv" / "bin" / "python3.13"
PINTEREST_PYTHON = PINTEREST_DIR / "venv" / "bin" / "python3"

StageCallback = Callable[[str], None]
# (step_atual, total_steps, segundos_restantes_estimados | None)
ProgressCallback = Callable[[int, int, Optional[int]], None]


class PipelineError(Exception):
    """Erro esperado do pipeline (venv faltando, scraper falhou, etc.)."""


def _noop(_msg: str) -> None:
    return None


def _noop_progress(_current: int, _total: int, _eta_seconds: Optional[int]) -> None:
    return None


# Linha de progresso do tqdm usada pelo mflux, ex:
#  "12%|██████████▋                  | 1/8 [03:15<22:45, 195.10s/it]"
_TQDM_RE = re.compile(
    r"(\d+)%\|[^|]*\|\s*(\d+)/(\d+)\s*\[([\d:]+)<([\d:]+)"
)


def _parse_hms(text: str) -> Optional[int]:
    """'22:45' ou '1:02:45' -> segundos. None se não for um tempo válido."""
    try:
        parts = [int(p) for p in text.strip().split(":")]
    except ValueError:
        return None
    seconds = 0
    for p in parts:
        seconds = seconds * 60 + p
    return seconds


def _stream_subprocess(cmd, cwd: str, env: dict, on_line: Callable[[str], None]) -> int:
    """Roda um subprocesso e entrega cada linha (inclusive as atualizadas via
    \\r, como as barras de progresso do tqdm) pra on_line, em tempo real.
    Devolve o código de saída.
    """
    proc = subprocess.Popen(
        cmd, cwd=cwd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert proc.stdout is not None
    buf = ""
    while True:
        ch = proc.stdout.read(1)
        if ch == "":
            break
        if ch in ("\r", "\n"):
            if buf:
                on_line(buf)
            buf = ""
        else:
            buf += ch
    if buf:
        on_line(buf)
    return proc.wait()


def run_scraper(url: str, on_stage: StageCallback = _noop) -> dict:
    """Roda o scraper3.py oficial e devolve o dict salvo em produto.json.

    Levanta PipelineError em vez de sys.exit — seguro pra chamar de dentro
    de um servidor de longa duração (não derruba o processo inteiro).
    """
    if not SCRAPER_PYTHON.exists():
        raise PipelineError(f"venv do sehin-scrapper não encontrado em {SCRAPER_PYTHON}")

    on_stage("Abrindo o Chrome e buscando o produto na Shein…")
    print(f"\n[pipeline] === Etapa 1/2: scraping da Shein ===")
    print(f"[pipeline] URL: {url}")
    print(
        "[pipeline] Uma janela do Chrome vai abrir (a Shein bloqueia acesso "
        "sem navegador real). Se cair numa tela de captcha, resolva na janela "
        "e aguarde o script continuar."
    )

    result = subprocess.run(
        [str(SCRAPER_PYTHON), "scraper3.py", url],
        cwd=str(SCRAPER_DIR),
    )
    if result.returncode != 0:
        raise PipelineError(f"scraper3.py terminou com código {result.returncode}")

    produto_json = SCRAPER_DIR / "downloads" / "produto.json"
    if not produto_json.exists():
        raise PipelineError(f"{produto_json} não foi gerado pelo scraper.")

    data = json.loads(produto_json.read_text(encoding="utf-8"))

    if not data.get("product_id"):
        raise PipelineError("scraper não conseguiu extrair o ID do produto da URL.")
    if not data.get("image_saved_to"):
        err = data.get("image_download_error", "motivo desconhecido")
        raise PipelineError(f"scraper não baixou a imagem do produto ({err}).")

    on_stage(f"Produto encontrado: {data.get('name') or data['product_id']}")
    return data


def run_pinterest_inspo(query: str, n: int = 16, on_stage: StageCallback = _noop) -> List[bytes]:
    """Busca `n` fotos de inspiração no Pinterest a partir de uma descrição em
    texto (ex: "vestido animal print") e devolve o conteúdo (bytes) de cada
    imagem já baixada — pronto pra virar data URL/base64 do lado do server.

    Levanta PipelineError se a busca falhar ou não achar nada.
    """
    if not PINTEREST_PYTHON.exists():
        raise PipelineError(f"venv do pinterest-inspo não encontrado em {PINTEREST_PYTHON}")

    on_stage(f'Buscando "{query}" no Pinterest…')

    tmp_dir = Path(tempfile.mkdtemp(prefix="inspo_"))
    try:
        cmd = [
            str(PINTEREST_PYTHON), "pinterest_inspo.py", query,
            "--n", str(n),
            "--output-dir", str(tmp_dir),
        ]
        result = subprocess.run(cmd, cwd=str(PINTEREST_DIR), capture_output=True, text=True)
        if result.returncode != 0:
            err_tail = (result.stderr or result.stdout or "").strip()[-400:]
            raise PipelineError(f"pinterest_inspo.py terminou com código {result.returncode}: {err_tail}")

        files = sorted(tmp_dir.glob("inspo_*.jpg"))
        if not files:
            raise PipelineError("nenhuma imagem de inspiração encontrada pra essa busca.")

        on_stage(f"{len(files)} imagem(ns) encontradas.")
        return [f.read_bytes() for f in files]
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def run_garment_reconstructor(
    image_path: Path,
    peca: Optional[str],
    steps: int,
    seed: int,
    quantize: int,
    low_ram: bool,
    on_stage: StageCallback = _noop,
    on_progress: ProgressCallback = _noop_progress,
) -> Path:
    """Roda reconstruct.py sobre a imagem baixada e devolve o path do PNG final.

    reconstruct.py chama o `mflux` (que usa tqdm) sem capturar a saída dele,
    então o stdout do mflux passa direto pelo descritor de arquivo herdado —
    capturando o stdout do processo reconstruct.py aqui a gente já recebe as
    linhas de progresso do tqdm em tempo real, sem precisar mexer no
    garment-reconstructor.
    """
    if not GARMENT_PYTHON.exists():
        raise PipelineError(f"venv do garment-reconstructor não encontrado em {GARMENT_PYTHON}")

    on_stage("Segmentando a peça…")
    print(f"\n[pipeline] === Etapa 2/2: tratamento da foto (garment-reconstructor) ===")
    print(f"[pipeline] Imagem de entrada: {image_path}")

    cmd = [
        str(GARMENT_PYTHON), "reconstruct.py", str(image_path),
        "--steps", str(steps),
        "--seed", str(seed),
        "--quantize", str(quantize),
    ]
    if peca:
        cmd += ["--peca", peca]
    if low_ram:
        cmd.append("--low-ram")

    # Garante que o `mflux` (instalado via `uv tool install`, normalmente em
    # ~/.local/bin) seja encontrado mesmo que o shell atual não tenha esse
    # diretório no PATH. PYTHONUNBUFFERED evita que prints do reconstruct.py
    # fiquem presos em buffer quando o stdout não é um terminal.
    env = os.environ.copy()
    local_bin = str(Path.home() / ".local" / "bin")
    env["PATH"] = local_bin + os.pathsep + env.get("PATH", "")
    env["PYTHONUNBUFFERED"] = "1"

    started_mflux = False

    def handle_line(line: str):
        nonlocal started_mflux
        print(line)  # mantém o log completo no console/terminal, igual antes

        m = _TQDM_RE.search(line)
        if m:
            if not started_mflux:
                started_mflux = True
                on_stage("Gerando a foto de produto com o FLUX.2 (mflux)…")
            current, total = int(m.group(2)), int(m.group(3))
            eta_seconds = _parse_hms(m.group(5))
            on_progress(current, total, eta_seconds)

    returncode = _stream_subprocess(cmd, str(GARMENT_DIR), env, handle_line)
    if returncode != 0:
        raise PipelineError(f"reconstruct.py terminou com código {returncode}")

    out_path = GARMENT_DIR / "output" / f"{image_path.stem}_produto.png"
    if not out_path.exists():
        raise PipelineError(f"esperava encontrar o resultado em {out_path}, mas não foi gerado.")

    return out_path


def process(
    url: str,
    peca: Optional[str] = None,
    steps: int = 6,
    seed: int = 123,
    quantize: int = 8,
    low_ram: bool = False,
    on_stage: StageCallback = _noop,
    on_progress: ProgressCallback = _noop_progress,
) -> dict:
    """Roda o pipeline completo (link -> id + sku + foto tratada) e devolve
    um dict com o resultado. É isso que server.py chama por trás da fila.

    on_progress é chamado só durante a etapa de geração da imagem (mflux),
    com (step_atual, total_steps, segundos_restantes_estimados).

    Levanta PipelineError em caso de falha esperada (venv faltando, scraper
    bloqueado, etc.) — quem chamar decide como reportar isso (job de fila,
    CLI, etc.).
    """
    # --- Etapa 1: Shein -> ID + foto ---
    produto = run_scraper(url, on_stage=on_stage)
    product_id = produto["product_id"]
    downloaded_image = Path(produto["image_saved_to"])

    # --- Copia a foto baixada pro input/ do garment-reconstructor ---
    GARMENT_DIR.joinpath("input").mkdir(exist_ok=True)
    staged_image = GARMENT_DIR / "input" / f"{product_id}.jpg"
    shutil.copy(downloaded_image, staged_image)

    # --- Etapa 2: foto -> foto tratada ---
    treated_image = run_garment_reconstructor(
        staged_image, peca, steps, seed, quantize, low_ram,
        on_stage=on_stage, on_progress=on_progress,
    )

    # --- Saída final: output/<product_id>/ nesta pasta ---
    dest_dir = FINAL_OUTPUT_DIR / product_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    final_image_path = dest_dir / f"{product_id}.png"
    shutil.copy(treated_image, final_image_path)

    result = {
        "product_id": product_id,
        "sku": produto.get("sku"),
        "name": produto.get("name"),
        "source_url": produto.get("source_url"),
        "final_image": str(final_image_path),
    }
    (dest_dir / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    on_stage("Concluído.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("url", help="Link do produto na Shein")
    parser.add_argument("--peca", default=None, help="vestido, blusa, saia, calca, short, cinto, sapato, bolsa, lenco, chapeu (padrão: auto)")
    parser.add_argument("--steps", type=int, default=6)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--quantize", type=int, default=8, choices=[3, 4, 5, 6, 8])
    parser.add_argument("--low-ram", action="store_true")
    args = parser.parse_args()

    try:
        result = process(
            args.url, args.peca, args.steps, args.seed, args.quantize, args.low_ram,
            on_stage=lambda msg: print(f"[pipeline] {msg}"),
        )
    except PipelineError as e:
        print(f"[pipeline] ERRO: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n[pipeline] === Concluído ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
