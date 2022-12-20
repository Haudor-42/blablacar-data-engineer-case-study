# Analytic data model documentation

Relating to the data model schema `analytic_data_model.pdf`, here's a complete documentation that data analysts & business analysts could use when consuming those tables.
It contains table description, relationships explanation, columns documentation added to partition / clustering informations.

For your information analysts:

- **Partitioning** a table involves dividing the data into smaller, more manageable pieces called partitions. This can improve query performance by allowing you to access only the data you need, rather than reading the entire table.
- **Clustering** a table involves organizing the data based on the values in one or more columns. This can improve query performance by allowing you to access rows with similar values more efficiently.

## Users

This table would contain information about users, them being driver or passenger and includes every information required during the registration on BlaBlaCar application.

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| pk_user_id | INTEGER | NO | Unique identifier for users. Primary key of the table. |
| first_name | STRING | NO | First name of the user. |
| last_name | STRING | NO | Last name of the users. |
| email | STRING | NO | Email address of the user. |
| birthdate | DATE | NO | Birthdate of the user. |
| phone | STRING | NO | Phone number of the user. |
| gender | STRING | YES | Gender of the user. This can be NULL as the user can omit this information when registering. |
| profile_picture | STRING | YES | Path to the uploaded profile picture. This can be NULL as the user can omit this information when registering. |
| phone_confirmed | BOOLEAN | NO | Flag if the user has confirmed the given phone number. Default to FALSE. |
| email_confirmed | BOOLEAN | NO | Flag if the user has confirmed the given email address. Default to FALSE. |
| identity_document_confirmed | BOOLEAN | NO | Flag if the user has confirmed his identity providing the required documents. Default to FALSE. |
| minibio | STRING | YES | Custom mini biography provided by the user. This can be NULL as the user can omit this information when registering. |
| discussion_preference_level | INTEGER | NO | This relate to discussion preferences which can be: (1) I'm a real chatterbox! / (2) I like to talk when I feel comfortable / (3) I am rather discreet. Defaults to (2). |
| music_preference_level | INTEGER | NO | This relate to music preferences which can be: (1) Music all the way! / (2) It depends on the music / (3) Silence is golden! Defaults to (2). |
| smoking_preference_level | INTEGER | NO | This relate to smoking preferences which can be: (1) I don't mind traveling with smokers / (2) I don't mind smoking breaks outside the car / (3) I do not travel with smokers. Defaults to (2). |
| animal_preference_level | INTEGER | NO | This relate to animal preferences which can be: (1) I love animals. Wow! / (2) I can travel with some animals / (3) I prefer not to travel with animals. Defaults to (2). |
| vaccinal_pass_preference_level | INTEGER | NO | This relate to vaccinal pass preferences which can be: (1) If necessary, I agree to show a vaccination pass at departure / (2) I do not want to show a vaccination pass at departure. Defaults to (2). |
| created_at_date | TIMESTAMP | NO | Creation date and time of the user. |
| last_updated_at_date | TIMESTAMP | NO | Last update date and time of the user. This equals the creation date and time on user creation. |
| deleted_at_date | TIMESTAMP | YES | Deletion date and time of the user. |


**Partitioning**: `created_at_date` by day --> This would allow an efficient access to users based on their account creation date.

**Clustering**: `phone_confirmed` + `email_confirmed` + `identity_document_confirmed` --> Improved performance when working with users that have confirmed their phone, email and identity. As those are more likely the best driver candidate, it could help finding them with ease.

**Relationships**:
- _Users_ can published one-to-many trip offers. A user that has published at least one trip offer can be considered as a Driver.
- _Users_ can perform one-to-many bookings. A user that has booked at least one trip/step of a trip can be considered as a Passenger.


## Trips

This table would contain information about trips, them being the materialization of a journey between a departure and an arrival that can be composed of one to many steps, if the trip has no stops between arrival and departure then it only has a single and unique step.

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| pk_trip_id | INTEGER | NO | Unique identifier for trips. Primary key of the table. |
| fk_driver_id | INTEGER | NO | Unique identifier for the driver that submitted the trip. Foreign key to the Users table. |
| departure_display_name | STRING | NO | Display name for the departure which can be for instance Nantes / Paris / Toulouse / ... |
| departure_country | STRING | NO | Country in which the departure takes place. |
| departure_gps_coordinate | STRING | NO | GPS coordinate for the departure. GPS coordinates are stored as a GeoJSON object. Example: { "type": "Point", "coordinates": [40.7128, -74.0060] } |
| departure_date | DATE | NO | Departure date chosen by the driver. |
| departure_time | TIME | NO | Departure time chosen by the driver. |
| arrival_display_name | STRING | NO | Display name for the arrival which can be for instance Nantes / Paris / Toulouse / ... |
| arrival_country | STRING | NO | Country in which the arrival takes place. |
| arrival_gps_coordinate | STRING | NO | GPS coordinate for the arrival. GPS coordinates are stored as a GeoJSON object. Example: { "type": "Point", "coordinates": [40.7128, -74.0060] } |
| favourite_route | STRING | NO | Favorite route selected by the driver when submitting the trip. |
| number_of_seats | INTEGER | NO | Number of seats available for the trip. |
| booking_without_approval | BOOLEAN | NO | Flag if the driver allows passenger to book trip directly or if he desire to explicitly perform a review. |
| price_per_seat | INTEGER | NO | Price per seat chosen by the driver. It can be the one proposed by the application or a custom one. |
| return_trip | BOOLEAN | NO | Flag if the driver intend to perform a return trip. |
| insured_trip | BOOLEAN | NO | Flag if the driver chose to insure the trip. |
| additional_information | STRING | YES | Optional text box in which driver can communicate information about the trip. |
| created_at_date | TIMESTAMP | NO | Creation date and time of the trip. |
| last_updated_at_date | TIMESTAMP | NO | Last update date and time of the trip. This equals the creation date and time on trip creation. |
| deleted_at_date | TIMESTAMP | YES | Deletion date and time of the trip. |


**Partitioning**: `departure_date by day` --> This would be a good way to organize the data given the possibility we are very likely to analyze the data fetching trips per departure time.

**Clustering**: `departure_country` --> This could be an efficient way to gather data that concerns specific country so it's easy to compute metrics per country.

Given the analytic needs, this might be debatable and adapted to suit the analysts needs and usages but I did not have enough hindsight on the matter.

**Relationships**:
- _Trips_ can be composed of one to many steps. If there are no step between the departure and the arrival, only a single step will be associated to the trip.
- _Trips_ can be booked by one to many passengers given the number of available seats.
- _Trips_ are published by one user considered then as a Driver.


## Steps

This table would contain information about the steps that compose a trip. A trip has at least one step between the departure and the arrival but can have many more.

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| pk_step_id | INTEGER | NO | Unique identifier for steps. Primary key of the table. |
| fk_trip_id | INTEGER | NO | Unique identifier for the trip which the step refers to. Foreign key to the Trips table. |
| departure_gps_coordinate | STRING | NO | GPS coordinate for the departure. GPS coordinates are stored as a GeoJSON object. Example: { "type": "Point", "coordinates": [40.7128, -74.0060] } |
| arrival_gps_coordinate | STRING | NO | GPS coordinate for the departure. GPS coordinates are stored as a GeoJSON object. Example: { "type": "Point", "coordinates": [40.7128, -74.0060] } |
| price_per_seat | INTEGER | NO | Price per seat chosen by the driver. It can be the one proposed by the application or a custom one. |


**Partitioning**: I don't think there's a good partitioning idea here but I may be wrong.

**Clustering**: `fk_trip_id` --> I guess this could be the only good clustering key but I'm not sure it would be really efficient/usable.

**Relationships**:
- _Steps_ are always related to a single trip.
- _Steps_ can result in one to many bookings depending of the number of seats available for the trip.


## Bookings

This table would contain information about the bookings which are the result of passengers reserving all or part of a trip which therefore means they reserve one or more steps of a single trip.

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| pk_booking_id | INTEGER | NO | Unique identifier for bookings. Primary key of the table. |
| fk_trip_id | INTEGER | NO | Unique identifier for the trip. Foreign key to the Trips table. |
| fk_step_id | INTEGER | NO | Unique identifier for the step. Foreign key to the Steps table. |
| fk_passenger_id | INTEGER | NO | Unique identifier for the passenger. Foreign key to the Users table. |
| number_of_seats | INTEGER | NO | Number of seat booked by the passenger. |
| total_price | INTEGER | NO | Result of the price per seat for the given step multiplied by the number of seats booked. |


**Partitioning**: I don't think there's a good partitioning idea here but I may be wrong.

**Clustering**: `fk_trip_id` --> I guess this could be the only good clustering key but I'm not sure it would be really efficient/usable.

**Relationships**:
- _Bookings_ are always related to a single trip.
- _Bookings_ are always related to a single step of a trip and this step can be the departure and arrival of the trip.
- _Bookings_ are always related to a single passenger.
