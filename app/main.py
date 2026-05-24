import json
import logging
import os
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import boto3
import psycopg
from psycopg.rows import dict_row

from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.audio_endpoint_url,
        aws_access_key_id=settings.audio_access_key_id,
        aws_secret_access_key=settings.audio_secret_access_key,
        region_name=settings.audio_region,
    )


def claim_next_job(conn) -> dict | None:
    sql = """
    with next_job as (
        select id
        from echo_lab_jobs
        where status = 'queued'
        order by created_at asc
        for update skip locked
        limit 1
    )
    update echo_lab_jobs job
    set
        status = 'processing',
        started_at = now(),
        attempt_count = coalesce(attempt_count, 0) + 1,
        error_message = null
    from next_job
    where job.id = next_job.id
    returning
        job.id,
        job.user_id,
        job.audio_key,
        job.expected_text,
        job.expected_jyutping;
    """

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()


def mark_completed(conn, job_id: str):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                update echo_lab_jobs
                set status = 'completed',
                    completed_at = now(),
                    error_message = null
                where id = %s
                """,
                (job_id,),
            )


def mark_failed(conn, job_id: str, error: str):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                update echo_lab_jobs
                set status = 'failed',
                    failed_at = now(),
                    error_message = %s
                where id = %s
                """,
                (error[:2000], job_id),
            )


def save_result(conn, job: dict, result: dict):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into echo_lab_results (
                    job_id,
                    user_id,
                    overall_score,
                    tone_score,
                    acoustic_tone_score,
                    reference_tone_score,
                    transcript,
                    feedback,
                    raw_result,
                    created_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                on conflict (job_id)
                do update set
                    overall_score = excluded.overall_score,
                    tone_score = excluded.tone_score,
                    acoustic_tone_score = excluded.acoustic_tone_score,
                    reference_tone_score = excluded.reference_tone_score,
                    transcript = excluded.transcript,
                    feedback = excluded.feedback,
                    raw_result = excluded.raw_result
                """,
                (
                    job["id"],
                    job["user_id"],
                    result["overall_score"],
                    result["tone_score"],
                    result["acoustic_tone_score"],
                    result["reference_tone_score"],
                    result["transcript"],
                    result["feedback"],
                    json.dumps(result["raw_result"]),
                ),
            )


def convert_to_wav(input_path: Path, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        str(output_path),
    ]

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {completed.stderr[-1000:]}")


def process_audio(job: dict, wav_path: Path) -> dict:
    file_size = wav_path.stat().st_size

    return {
        "overall_score": 72,
        "tone_score": 68,
        "acoustic_tone_score": None,
        "reference_tone_score": None,
        "transcript": None,
        "feedback": "Echo Labs processed this recording. Real scoring is not wired in yet.",
        "raw_result": {
            "processor": "placeholder-v1",
            "audio_file_size_bytes": file_size,
            "expected_text": job.get("expected_text"),
            "expected_jyutping": job.get("expected_jyutping"),
        },
    }


def process_job(conn, job: dict):
    logger.info("Processing Echo Labs job %s", job["id"])

    s3 = get_s3_client()
    run_id = uuid4().hex

    work_dir = Path(settings.local_tmp_dir) / str(job["id"]) / run_id
    input_path = work_dir / "input.audio"
    wav_path = work_dir / "normalised.wav"

    work_dir.mkdir(parents=True, exist_ok=True)

    s3.download_file(
        settings.audio_bucket_name,
        job["audio_key"],
        str(input_path),
    )

    convert_to_wav(input_path, wav_path)

    result = process_audio(job, wav_path)

    save_result(conn, job, result)
    mark_completed(conn, str(job["id"]))

    logger.info("Completed Echo Labs job %s", job["id"])


def run_forever():
    logger.info("Echo Labs worker booted")
    logger.info("Worker PID: %s", os.getpid())

    while True:
        try:
            with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
                job = claim_next_job(conn)

                if job is None:
                    logger.info("No queued Echo Labs jobs found")
                    time.sleep(settings.worker_poll_seconds)
                    continue

                try:
                    process_job(conn, job)
                except Exception as error:
                    logger.exception("Failed job %s", job["id"])
                    mark_failed(conn, str(job["id"]), str(error))

        except Exception:
            logger.exception("Worker loop failed before claiming a job")
            time.sleep(settings.worker_poll_seconds)


if __name__ == "__main__":
    run_forever()