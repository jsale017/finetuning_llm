import os
import asyncio
import json
from dotenv import load_dotenv
from llama_cloud_services import LlamaExtract
from llama_agent_creation import agent
from llama_cloud.core.api_error import ApiError

# Load environment variables from .env file
load_dotenv()

async def main():

    pdf_files = [
        "data/ASC 105.pdf", "data/ASC 205.pdf", "data/ASC 210.pdf",
        "data/ASC 215.pdf", "data/ASC 220.pdf", "data/ASC 225.pdf",
        "data/ASC 230.pdf", "data/ASC 235.pdf", "data/ASC 250.pdf",
        "data/ASC 255.pdf", "data/ASC 260.pdf", "data/ASC 270.pdf",
        "data/ASC 280.pdf", "data/ASC 305.pdf", "data/ASC 310.pdf",
        "data/ASC 330.pdf", "data/ASC 340.pdf", "data/ASC 350.pdf",
        "data/ASC 360.pdf", "data/ASC 405.pdf", "data/ASC 410.pdf",
        "data/ASC 420.pdf", "data/ASC 440.pdf", "data/ASC 450.pdf",
        "data/ASC 460.pdf", "data/ASC 470.pdf", "data/ASC 480.pdf",
        "data/ASC 505.pdf", "data/ASC 606.pdf", "data/ASC 705.pdf",
        "data/ASC 710.pdf", "data/ASC 712.pdf", "data/ASC 715.pdf",
        "data/ASC 718.pdf", "data/ASC 730.pdf", "data/ASC 740.pdf",
        "data/ASC 805.pdf", "data/ASC 808.pdf", "data/ASC 810.pdf",
        "data/ASC 815.pdf", "data/ASC 820.pdf", "data/ASC 830.pdf",
        "data/ASC 835.pdf", "data/ASC 842.pdf", "data/ASC 855.pdf",
    ]

    # we spamming free api so we need to queue the jobs in batches
    batch_size = 5
    delay_seconds = 30
    max_retries = 3
    retry_delay_on_ratelimit = 60

    all_jobs = []
    print(f"Queueing {len(pdf_files)} PDF files for extraction in batches...")

    i = 0
    while i < len(pdf_files):
        batch_files = pdf_files[i:i + batch_size]
        print(f"Queueing batch {i//batch_size + 1}/{(len(pdf_files) + batch_size - 1) // batch_size} with {len(batch_files)} files...")
        
        retries = 0
        batch_successful = False
        while not batch_successful and retries < max_retries:
            try:
                jobs = await agent.queue_extraction(batch_files)
                all_jobs.extend(jobs)
                print("Extraction jobs for batch queued.")
                batch_successful = True
            except ApiError as e:
                if e.status_code == 429:
                    retries += 1
                    print(f"Rate limit exceeded (429). Retrying batch in {retry_delay_on_ratelimit} seconds... (Attempt {retries}/{max_retries})")
                    await asyncio.sleep(retry_delay_on_ratelimit)
                else:
                    print(f"An API error occurred: {e}")
                    raise
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                raise

        if not batch_successful:
            print(f"Failed to queue batch {i//batch_size + 1} after {max_retries} retries due to rate limits. Please try running the script again later.")
            break

        i += batch_size
        if i < len(pdf_files):
            print(f"Waiting for {delay_seconds} seconds before the next batch...")
            await asyncio.sleep(delay_seconds)

    print(f"Total {len(all_jobs)} extraction jobs queued across all batches.")

    print("Checking job statuses...")
    for job in all_jobs:
        job_status_retries = 0
        job_status = None
        while job_status is None and job_status_retries < max_retries:
            try:
                job_status = (await agent.get_extraction_job(job.id)).status
                print(f"Job {job.id} status: {job_status}")
            except ApiError as e:
                if e.status_code == 429:
                    job_status_retries += 1
                    print(f"Rate limit exceeded (429) while fetching status for job {job.id}. Retrying in {retry_delay_on_ratelimit} seconds... (Attempt {job_status_retries}/{max_retries})")
                    await asyncio.sleep(retry_delay_on_ratelimit)
                else:
                    print(f"An API error occurred fetching status for job {job.id}: {e}")
                    break
            except Exception as e:
                print(f"An unexpected error occurred fetching status for job {job.id}: {e}")
                break


    print("Fetching extraction results...")
    final_results = []
    for job in all_jobs:
        result_retries = 0
        job_result = None
        while job_result is None and result_retries < max_retries:
            try:
                job_result = await agent.get_extraction_run_for_job(job.id)
                final_results.append(job_result)
            except ApiError as e:
                if e.status_code == 429:
                    result_retries += 1
                    print(f"Rate limit exceeded (429) while fetching result for job {job.id}. Retrying in {retry_delay_on_ratelimit} seconds... (Attempt {result_retries}/{max_retries})")
                    await asyncio.sleep(retry_delay_on_ratelimit)
                else:
                    print(f"An API error occurred fetching result for job {job.id}: {e}")
                    break
            except Exception as e:
                print(f"An unexpected error occurred fetching result for job {job.id}: {e}")
                break

    for result in final_results:
        print(f"Result for job {result.job_id}:")
        print(json.dumps(result.data, indent=4))

        output_dir = "extracted_json_output"
        os.makedirs(output_dir, exist_ok=True)
        output_filename = os.path.join(output_dir, f"output_{result.job_id}.json")
        with open(output_filename, "w") as f:
            json.dump(result.data, f, indent=4)
        print(f"Saved result to {output_filename}")


if __name__ == "__main__":
    asyncio.run(main())
