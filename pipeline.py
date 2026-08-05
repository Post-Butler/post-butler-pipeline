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

Uso:
    python3 pipeline.py "<link-do-produto-shein>" [--peca short] [--steps 8]
        [--seed 123] [--quantize 8] [--low-ram]

Requisitos (nada disso é instalado por este script):
    - venv de sehin-scrapper já configurado (playwright/seleniumbase etc.)
    - venv de garment-reconstructor já configurado (torch/transformers etc.)
    - `mflux` instalado globalmente (uv tool install --upgrade mflux) e
      `~/.local/bin` acessível (este script já garante isso no PATH do
      subprocesso, mesmo que o shell atual não tenha).
"""

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
SCRAPER_DIR = ROOT / "sehin-scrapper"
GARMENT_DIR = ROOT / "garment-reconstructor"
FINAL_OUTPUT_DIR = ROOT / "output"

SCRAPER_PYTHON = SCRAPER_DIR / "venv" / "bin" / "python3.12"
GARMENT_PYTHON = GARMENT_DIR / "venv" / "bin" / "python3.13"


def die(msg: str) -> None:
    print(f"[pipeline] ERRO: {msg}", file=sys.stderr)
    sys.exit(1)


def run_scraper(url: str) -> dict:
    """Roda o scraper3.py oficial e devolve o dict salvo em produto.json."""
    if not SCRAPER_PYTHON.exists():
        die(f"venv do sehin-scrapper não encontrado em {SCRAPER_PYTHON}")

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
        die(f"scraper3.py terminou com código {result.returncode}")

    produto_json = SCRAPER_DIR / "downloads" / "produto.json"
    if not produto_json.exists():
        die(f"{produto_json} não foi gerado pelo scraper.")

    data = json.loads(produto_json.read_text(encoding="utf-8"))

    if not data.get("product_id"):
        die("scraper não conseguiu extrair o ID do produto da URL.")
    if not data.get("image_saved_to"):
        err = data.get("image_download_error", "motivo desconhecido")
        die(f"scraper não baixou a imagem do produto ({err}).")

    return data


def run_garment_reconstructor(
    image_path: Path,
    peca: str | None,
    steps: int,
    seed: int,
    quantize: int,
    low_ram: bool,
) -> Path:
    """Roda reconstruct.py sobre a imagem baixada e devolve o path do PNG final."""
    if not GARMENT_PYTHON.exists():
        die(f"venv do garment-reconstructor não encontrado em {GARMENT_PYTHON}")

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
    # diretório no PATH.
    import os
    env = os.environ.copy()
    local_bin = str(Path.home() / ".local" / "bin")
    env["PATH"] = local_bin + os.pathsep + env.get("PATH", "")

    result = subprocess.run(cmd, cwd=str(GARMENT_DIR), env=env)
    if result.returncode != 0:
        die(f"reconstruct.py terminou com código {result.returncode}")

    out_path = GARMENT_DIR / "output" / f"{image_path.stem}_produto.png"
    if not out_path.exists():
        die(f"esperava encontrar o resultado em {out_path}, mas não foi gerado.")

    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("url", help="Link do produto na Shein")
    parser.add_argument("--peca", default=None, help="vestido, blusa, saia, calca, short, cinto, sapato, bolsa, lenco, chapeu (padrão: auto)")
    parser.add_argument("--steps", type=int, default=8)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--quantize", type=int, default=8, choices=[3, 4, 5, 6, 8])
    parser.add_argument("--low-ram", action="store_true")
    args = parser.parse_args()

    # --- Etapa 1: Shein -> ID + foto ---
    produto = run_scraper(args.url)
    product_id = produto["product_id"]
    downloaded_image = Path(produto["image_saved_to"])

    # --- Copia a foto baixada pro input/ do garment-reconstructor ---
    GARMENT_DIR.joinpath("input").mkdir(exist_ok=True)
    staged_image = GARMENT_DIR / "input" / f"{product_id}.jpg"
    shutil.copy(downloaded_image, staged_image)

    # --- Etapa 2: foto -> foto tratada ---
    treated_image = run_garment_reconstructor(
        staged_image, args.peca, args.steps, args.seed, args.quantize, args.low_ram
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

    print("\n[pipeline] === Concluído ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
