#!/usr/bin/env python3
"""
server.py — backend local do Post Studio
------------------------------------------
Serve o post_studio.html e expõe uma fila de jobs em background que rodam
o pipeline.py (Shein -> foto tratada + SKU) sem travar a interface.

Por que uma fila (e não paralelo)?
  O scraper abre um Chrome de verdade e o garment-reconstructor usa a GPU/
  MPS do Mac (mflux) — rodar vários ao mesmo tempo brigaria por memória e
  deixaria tudo mais lento/instável. Um worker só, processando um job por
  vez, é mais previsível. Você pode continuar editando o resto do post
  (fundo, inspos, textos) enquanto um job de outra página está na fila.

Uso:
    python3 server.py
    (abre em http://127.0.0.1:5050)

Requer só `flask` instalado (pip install flask) — o resto (scraper, mflux)
roda nos venvs de cada submodule, exatamente como o pipeline.py já faz.
"""

import base64
import queue
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

import pipeline

ROOT = Path(__file__).parent.resolve()
WEB_DIR = ROOT / "web"
HTML_FILE = "post_studio.html"

app = Flask(__name__, static_folder=None)

# ---------------------------------------------------------------------
# Fila de jobs (em memória — server local, uso pessoal, um usuário só)
# ---------------------------------------------------------------------
jobs = {}          # job_id -> dict
jobs_lock = threading.Lock()
job_queue = queue.Queue()


def _update_job(job_id: str, **fields):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(fields)


def _worker_loop():
    while True:
        job_id = job_queue.get()
        try:
            with jobs_lock:
                job = jobs.get(job_id)
            if job is None:
                continue

            _update_job(job_id, status="running", stage="Iniciando…", progress=None)

            def on_stage(msg, jid=job_id):
                # troca de etapa (scraping, segmentação, geração...) zera o
                # progresso numérico anterior, se houver.
                _update_job(jid, stage=msg, progress=None)

            def on_progress(current, total, eta_seconds, jid=job_id):
                _update_job(jid, progress={
                    "current_step": current,
                    "total_steps": total,
                    "percent": round(current / total * 100) if total else None,
                    "eta_seconds": eta_seconds,
                })

            result = pipeline.process(
                job["url"],
                peca=job["peca"],
                steps=job.get("steps", 6),
                seed=job.get("seed", 123),
                quantize=job.get("quantize", 8),
                low_ram=job.get("low_ram", False),
                on_stage=on_stage,
                on_progress=on_progress,
            )

            image_bytes = Path(result["final_image"]).read_bytes()
            image_b64 = base64.b64encode(image_bytes).decode("ascii")
            result["image_data_url"] = f"data:image/png;base64,{image_b64}"

            _update_job(job_id, status="done", stage="Concluído.", result=result, error=None)
        except pipeline.PipelineError as e:
            _update_job(job_id, status="error", error=str(e))
        except Exception as e:  # nunca deixa o worker morrer por um job ruim
            _update_job(job_id, status="error", error=f"Erro inesperado: {e}")
        finally:
            job_queue.task_done()


threading.Thread(target=_worker_loop, daemon=True).start()


# ---------------------------------------------------------------------
# Busca de inspiração no Pinterest — bem mais rápida (~10-20s) e sem
# disputa de GPU/Chrome do garment-reconstructor, então cada busca roda
# na sua própria thread em vez de entrar na fila do worker acima.
# ---------------------------------------------------------------------
inspo_jobs = {}
inspo_jobs_lock = threading.Lock()


def _update_inspo_job(job_id: str, **fields):
    with inspo_jobs_lock:
        if job_id in inspo_jobs:
            inspo_jobs[job_id].update(fields)


def _run_inspo_job(job_id: str, query: str, n: int):
    try:
        _update_inspo_job(job_id, status="running", stage="Buscando no Pinterest…")

        def on_stage(msg, jid=job_id):
            _update_inspo_job(jid, stage=msg)

        images_bytes = pipeline.run_pinterest_inspo(query, n=n, on_stage=on_stage)
        images = [
            f"data:image/jpeg;base64,{base64.b64encode(b).decode('ascii')}"
            for b in images_bytes
        ]
        _update_inspo_job(job_id, status="done", stage="Concluído.", result={"images": images}, error=None)
    except pipeline.PipelineError as e:
        _update_inspo_job(job_id, status="error", error=str(e))
    except Exception as e:
        _update_inspo_job(job_id, status="error", error=f"Erro inesperado: {e}")


# ---------------------------------------------------------------------
# Rotas da UI
# ---------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory(str(WEB_DIR), HTML_FILE)


# ---------------------------------------------------------------------
# API de jobs
# ---------------------------------------------------------------------
@app.route("/api/jobs", methods=["POST"])
def create_job():
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    peca = (data.get("peca") or "").strip() or None

    if not url:
        return jsonify({"error": "campo 'url' é obrigatório"}), 400

    job_id = uuid.uuid4().hex[:12]
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "url": url,
            "peca": peca,
            "status": "queued",
            "stage": "Na fila…",
            "progress": None,
            "result": None,
            "error": None,
            "created_at": time.time(),
            "queue_position": job_queue.qsize(),
        }
    job_queue.put(job_id)
    return jsonify(jobs[job_id]), 201


@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if job is None:
        return jsonify({"error": "job não encontrado"}), 404
    return jsonify(job)


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    with jobs_lock:
        return jsonify(list(jobs.values()))


# ---------------------------------------------------------------------
# API de busca de inspiração (Pinterest)
# ---------------------------------------------------------------------
@app.route("/api/inspo-jobs", methods=["POST"])
def create_inspo_job():
    data = request.get_json(force=True, silent=True) or {}
    query = (data.get("query") or "").strip()
    try:
        n = int(data.get("n") or 16)
    except (TypeError, ValueError):
        n = 16

    if not query:
        return jsonify({"error": "campo 'query' é obrigatório"}), 400

    job_id = uuid.uuid4().hex[:12]
    with inspo_jobs_lock:
        inspo_jobs[job_id] = {
            "id": job_id,
            "query": query,
            "status": "queued",
            "stage": "Na fila…",
            "result": None,
            "error": None,
            "created_at": time.time(),
        }
    threading.Thread(target=_run_inspo_job, args=(job_id, query, n), daemon=True).start()
    return jsonify(inspo_jobs[job_id]), 201


@app.route("/api/inspo-jobs/<job_id>", methods=["GET"])
def get_inspo_job(job_id):
    with inspo_jobs_lock:
        job = inspo_jobs.get(job_id)
    if job is None:
        return jsonify({"error": "job não encontrado"}), 404
    return jsonify(job)


if __name__ == "__main__":
    print("[server] Post Studio rodando em http://127.0.0.1:5050")
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
