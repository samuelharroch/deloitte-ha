"""
Run the batch pipeline end to end, stopping at the first failing step:
  1. generate a new batch into data/*.csv
  2. upload it to GCS
  3. load it into the bronze tables
  4. run Dataform from the main branch (silver, then gold)

Dataform runs the code pushed to GitHub, so push model changes before running this.
"""

from __future__ import annotations

import generate_batch
import load_to_bq
import run_dataform
import upload_to_gcs

STEPS = [
    ("Generate batch", generate_batch.main),
    ("Upload to GCS", upload_to_gcs.main),
    ("Load into BigQuery bronze", load_to_bq.main),
    ("Run Dataform", run_dataform.main),
]


def main() -> None:
    for number, (title, step) in enumerate(STEPS, start=1):
        print(f"\n=== Step {number}/{len(STEPS)}: {title} ===")
        step([])
    print("\nPipeline finished.")


if __name__ == "__main__":
    main()
