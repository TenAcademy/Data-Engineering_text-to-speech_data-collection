# KAFKA CLUSTERS

### An ETL data pipeline to to collect and extract vocal data, transform and load it to a data warehouse using Kafka clusters, Airflow, and spark for a text to speech conversion project

![](screenshots/image.png)

## Project details

**Table of contents**

- [Introduction](#introduction)
- [Overview](#overview)
- [Objective](#objective)
- [Requirements](#requirements)
- [Install](#install)
- [How to use](#examples)
- [End-to-end pipelines](#pipelines)
- [Notebooks](#notebooks)
- [Scripts](#scripts)
- [Test](#test)
- [Authors](#authors)

## Introduction

> Data is everywhere. In order to get the best out of it one needs to extract it from several sources, make required transformations and load it to a data warehouse for further analysis and explorations. This is where ETL data pipelines come to use.

> ETL stands for Extract, Transform and Load. An ETL tool extracts the data from different RDBMS source systems, real-time user interactions, and sever other sorts of transactions. Then the extracted data will be transformed using transformations that are almost always specific to the goal of the project like applying calculations, concatenating, analyzing, aggregating, etc. And then load the data to the data warehouse system. The data is loaded in the DW system in the form of dimension and fact tables, which can serve as the basis for which the business analyzers, bushiness intelligence officers, and machine learning teams can continue to work on with.

## Overview

> Our client [10 Academy](https://www.10academy.org/), recognizing the value of large data sets for speech-t0-text data sets and seeing the opportunity that there are many text corpora for Amharic and Swahili languages, and understanding that complex data engineering skills are valuable to our profile for employers, want to have a design and build a robust, large scale, fault-tolerant, highly available Kafka cluster that can be used to post a sentence and receive an audio file.
>
> Producing a tool that can be deployed to process posting and receiving text and audio files from and into a Kafka topic, apply transformation in a distributed manner, and load it into a data warehouse in a suitable format to train a speech-to-text model would do the required job.

## Objective

> The main objective of this week’s project is to build a data engineering pipeline that allows recording millions of Amharic and Swahili speakers reading digital texts in-app and on web platforms
>
> This can be achieved by building an end-to-end ETL data pipeline that will use Apache Kafka, Apache Spark, and Apache Airflow in order to receive user voice audio files, transform them and load them to a data warehouse that will later be used for text-to-speech conversion machine learning project.
>
> Users will be prompted with several different sentences and they will provide their corresponding audio by recording using the front-end user interface that is provided.

## Requirements

> Python 3.5 or above
>
> Docker and Docker compose
> 
> Pip
>
> kafka-python
>
> Zookeeper
>
> Apache airflow
>
> Apache kafka
>
> Apache Spark

## Install

### Installing the Kafka cluster application

```
git clone https://github.com/TenAcademy/Data-Engineering_text-to-speech_data-collection.git
cd Data-Engineering_text-to-speech_data-collection
pip install -r requirements.txt
python entry_point.py
```

## Examples

> ### Using the application

- One can start using the application by first running the . . . .

> ### Interacting with the front end

- First navigate to the . . . tab.

- Start by selecting one of the . . .

  > ![](screenshots/image-II.png)

> .
>
> .
>
> .
>
> .

## Pipelines

> The detailed use and implementation of the pipelines using Apache Airflow can be found in this pipelines folder.

## Notebooks

> All the notebooks that are used in this project including EDA, data cleaning and summarization are found here in the Notebooks folder.

## Scripts

> All the scripts and modules used for this project relating to interactions with the kafka, airflow, spark and data warehouse frameworks along with default parameters and values used will be found here, in the scripts folder.

## Tests

> All the unit and integration tests are found here in the tests folder.

## Authors

> 👤 **Birhanu Gebisa**
>
> - [Email](mailto:birhanugebisa@gmail.com)
> - [GitHub](https://github.com/BirhanuGebisa)
> - [LinkedIn](https://www.linkedin.com/in/birhanu-gebisa2721/)

> 👤 **Ekubazgi Gebremariam**
>
> - [Email](mailto:axutec14@gmail.com)
> - [GitHub](https://github.com/ekubay)
> - [LinkedIn](https://www.linkedin.com/in/fisseha-estifanos-109ba6199/)

> 👤 **Emtinan Salaheldin**
>
> - [Email](mailto:emtinan.s.e.osman@gmail.com)
> - [GitHub](https://github.com/emtinanseo)
> - [LinkedIn](https://www.linkedin.com/in/emtinan-elkhidir-osman-646242119/)

> 👤 **Fisseha Estifanos**
>
> - [Email](mailto:fisseha.137@gamil.com)
> - [GitHub](https://github.com/fisseha-estifanos)
> - [LinkedIn](https://www.linkedin.com/in/fisseha-estifanos-109ba6199/)
> - [Twitter](https://twitter.com/f0x__tr0t)

> 👤 **Natnael Masresha**

> - [Email](mailto:natnaelmasresha@gmail.com)
> - [GitHub](https://github.com/Nathnael12)
> - [LinkedIn](https://www.linkedin.com/in/natnael-masresha-39a69b185/)
> - [Twitter](https://twitter.com/natnaelmasresha)

> 👤 **Niyomukiza Thamar**
>
> - [Email](mailto:thamarniyo@gmail.com)
> - [GitHub](https://github.com/niyotham)
> - [LinkedIn](https://www.linkedin.com/in/fisseha-estifanos-109ba6199/)

## Show us your support

> Give us a ⭐ if you like this project, and also feel free to contact us at any moment.

![Contributors list](https://contrib.rocks/image?repo=TenAcademy/Data-Engineering_text-to-speech_data-collection)
