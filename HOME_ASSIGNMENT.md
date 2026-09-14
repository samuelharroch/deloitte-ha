# Home Assignment Requirements

You may use AI, as long as you can explain the exercise.

## Task

1. **Create data files** for a users table and an addresses table with at least 1,000 users (you may use [https://generatedata.com/](https://generatedata.com/)).

2. **Skewed city distribution:** In the addresses table, make sure one city accounts for half of the users.

3. **Alternative datasets:** If you find ready-made data that lets you do the same exercise (even if it is not users and addresses), that is also fine.

4. **Repositories:** Create a Git repo and a Dataform repo, and connect them to each other.

5. **Cloud Storage:** Load the files into a bucket.

6. **BigQuery load:** From the bucket, load the files into BigQuery.

7. **Incremental / upsert:** Create a Dataform job that updates the tables incrementally / with upsert.

8. **Join:** Create a Dataform job that runs and produces the JOIN between them.

9. **Dataproc aggregation:** Create a Dataproc process that reads the target table and summarizes how many users there are in each city.

10. **Prove skew in Spark UI:** Prove via the Spark UI that there is skew affecting shuffling because of the city with half of the users.

11. **Disable Dataproc optimizations:** Note that Dataproc will apply optimizations, so you will not see the skew unless you disable them.
