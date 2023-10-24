# Sanctions

## Open Sanctions
The [OpenSanctions Default](https://www.opensanctions.org/datasets/default/) dataset contains the most data
collected by open sanctions. 



### Data Description
We've downloaded the full dataset in the [FollowTheMoney](https://followthemoney.tech/) format on the 5th of october 2023. 
It contains a set of directly or indirectly targeted entities, who are sanctions by different actors.
Furthermore, we added the datasource information to the entity to find out by whom the entity is sanctions, 
because the original dataset only contains the datasource name with no information about the actor.  

We end up with a dataset of 3.139.628 entries. 
Each entry contains the following fields:
* `id`: The unique identifier for the entity
* `caption`: The name of the entity
* `schema`: The type of entity (e.g. Person, Associate, Company, ...)
* `first_seen`: First occurrence of the entry in a dataset
* `last_seen`: Last occurrence of the entry in a dataset
* `last_change`: Last time the entry was updated
* `target`: Is the entity directly a targeted in the datasource, if not, the entity is referred to a directly targeted entity like its political party or a referred address.   
* `dataset_names`: The dataset the entry was found in
* `datasets`: The information on the creator of the dataset (e.g., name country, ...)

The full description can be found at [OpenSanctions](https://www.opensanctions.org/reference/).

Since we are interested in the relationship between countries,
we have to extract the country's information from the entity. 

## GDELT Data

## Export & Import
Export the dataset from the database:
```bash
<pg_dump> -U postgres --dbname=<dbname> --schema='<schema>' --table=<schema>.<table> --file="<file>"
```
Import the dataset into the database:

```bash
<pg_dump> --dbname=<dbname> --schema='<schema>' --table=<schema>.<table> "<file>"
```