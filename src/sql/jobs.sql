-- jobs.sql
-- retrieve up to ten jobs that have been run in the last
-- seven days by humans (not service accounts), including
-- the job id, creation time, and query

SELECT
    job_log.job_id,
    job_log.creation_time,
    job_log.query,
FROM
    `region-europe-west2`.`INFORMATION_SCHEMA`.`JOBS` AS job_log
CROSS JOIN
    UNNEST(job_log.referenced_tables) AS referenced_table
WHERE
    DATE(job_log.creation_time) > DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
    AND job_log.state = 'DONE'
    AND job_log.job_type = 'QUERY'
    AND job_log.statement_type = 'SELECT'
    -- ignore service accounts
    AND REGEXP_CONTAINS(job_log.user_email, '.*@justpark.com$')
    AND referenced_table.dataset_id = @dataset_id
    AND referenced_table.table_id = @relation_id
ORDER BY
    RAND() -- we randomly select our results
LIMIT
    10 -- we avoid retrieving too many results
