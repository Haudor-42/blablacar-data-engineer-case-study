# Analytic data model query sample

In addition to the documentation containing table description, relationships explanation, columns documentation added to partition / clustering informations, here's a set of queries that can help you to begin you exploration and analysis. Enjoy :)

## Sample queries

### How many trip offers have been published last month?

Assuming that we are using the current_date as the starting point of the last month:

```
SELECT COUNT(pk_trip_id)
FROM Trips
WHERE created_at_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE()
AND deleted_at_date IS NULL;
```

This query counts the number of rows in the trips table that have a created_at_date within the past month (from the current date) and do not have a deleted_at_date value.

If you rather are looking for the trip offers that have been published in the last calendar month, this will be better:

```
SELECT COUNT(pk_trip_id)
FROM Trips
WHERE created_at_date BETWEEN FORMAT_DATE('%Y-%m-01', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AND DATE_ADD(CAST(FORMAT_DATE('%Y-%m-01', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS DATE), INTERVAL 1 MONTH)
AND deleted_at_date IS NULL;
```

This also will omit the deleted trips from the count but can be adapted easily deleting the following clause: `AND deleted_at_date IS NULL;`


### What country had the highest number of publications last month?

```
SELECT departure_country, COUNT(*) as num_publications
FROM trips
WHERE created_at_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE()
AND deleted_at_date IS NULL
GROUP BY departure_country
ORDER BY num_publications DESC
LIMIT 1;
```

This query returns the country with the highest number of publications last month by selecting the departure_country column and counting the number of rows for each country using the COUNT function and an AS clause to give the result a friendly name (num_publications). The WHERE clause filters the rows to include only those with a created_at_date within the past month and no deleted_at_date value.

The GROUP BY clause groups the rows by departure_country, and the ORDER BY clause sorts the resulting groups by num_publications in descending order using the DESC keyword. The LIMIT clause is used to return only the first row, which will be the country with the highest number of publications.

The resulting output will look something like this:

| departure_country |	num_publications |
| ----------------- | ---------------- |
| France | 50 |

Again if you need this information but for the past calendar month, the following query will be better:

```
SELECT departure_country, COUNT(*) as num_publications
FROM trips
WHERE created_at_date BETWEEN FORMAT_DATE('%Y-%m-01', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AND DATE_ADD(CAST(FORMAT_DATE('%Y-%m-01', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS DATE), INTERVAL 1 MONTH)
AND deleted_at_date IS NULL
GROUP BY departure_country
ORDER BY num_publications DESC
LIMIT 1;
```
