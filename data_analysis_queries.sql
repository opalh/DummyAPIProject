# How many new users are added daily?
SELECT
  CAST(registerDate AS date) AS date_added,
  COUNT(*)
FROM dummyApi.users
GROUP BY CAST(registerDate AS date);

# What is the average time between registration and first comment?
SELECT
  AVG( TIMESTAMPDIFF(minute,
      b.publishDate,
      a.registerDate))
FROM dummyApi.users a
INNER JOIN dummyApi.comments b
ON a.id=ownerId;

# Which cities have the most activity, in terms of posts per day?

WITH
  ctx AS(
  SELECT
    b.loc_city AS city,
    CAST(a.publishDate AS date) AS datevalue,
    COUNT(a.id) AS posts
  FROM
    dummyApi.posts a
  INNER JOIN
    dummyApi.users b
  ON
    a.ownerId=b.id
  GROUP BY
    b.loc_city,
    CAST(a.publishDate AS date))
SELECT city, MAX(posts)
FROM ctx
GROUP BY city
ORDER BY MAX(posts) DESC;

# Which tags are most frequently encountered, across user posts?
WITH
  raw AS (
  SELECT
    id,
    REPLACE(REPLACE(REPLACE(tags, '"', ''),'[',''),']','') AS tags
  FROM
    dummyApi.posts ),
  PIVOT AS (
  SELECT
    id,
    TRIM(substring_index(tags,',',1)) AS tag
  FROM
    raw
  UNION ALL
  SELECT
    id,
    TRIM(SUBSTRING_INDEX(substring_index(tags,',',2),',',-1) ) AS tag
  FROM
    raw
  UNION ALL
  SELECT
    id,
    TRIM(substring_index(tags,',',-1) ) AS tag
  FROM
    raw )
SELECT
  tag, COUNT(id) AS cnt
FROM PIVOT
GROUP BY tag
ORDER BY cnt DESC;