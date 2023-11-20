# Sanctions Dashboard

## Introduction
In recent years, international sanctions have gained prominence due to events such as Russia's war against Ukraine and 
the corresponding [sanctions on Russian oil](https://www.derstandard.at/story/2000143240237/neue-oel-sanktionen-der-eu-gegen-russland-in-kraft-getreten)
as well as the ban of [Google services on Huawei devices](https://www.derstandard.at/story/2000103448324/huawei-verliert-android-lizenz-und-zugang-zum-play-store). 

## Dataset
We use the [OpenSanctions Default](https://www.opensanctions.org/datasets/default) dataset containing over three million different entries. 
It is an aggregation of > 100 datasources created by sanctioning actors. 
If a record is part of a datasource, it means it is sanctioned by this actor. 
These datasources are published by countries, unions (e.g. the European Union / UN) or other institutions like NGOs or
Interpol.

The records are stored in the [FtM](https://followthemoney.tech) format containing the following fields: 
* `id`: Unique Identifier
* `caption`: Additional info on the sanctioned entry
* `datasets`: The datasets in which this entry is listed. (Therefore, this contains the info by whom this record is sanctioned) 
* `first_seen`, `last_seen`, `last_changed`: When this record appeared for the first-/ last-time or was changed in one of the datasets
* `schema`: Describing the records' type and the structure of the properties (A schema can be a company, a natural person, a bank account, etc.)
* `properties`: More detailed info on the record (A person has a nationality, address, ..., whereas a company has a national identification/ tax number)

Furthermore,
we intend to join this data with the [Orbis](https://www.bvdinfo.com/en-gb/our-products/data/international/orbis) 
database to find the industries of the companies.

## The Tool
The tool is a dashboard to visualize the data and provide various statistics. 