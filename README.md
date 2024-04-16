# Stack Exchange Data Insights

This project enables you to download, process, and analyze data from the [Stack Exchange](https://stackexchange.com/) network, providing valuable insights into various topics discussed on the platform.

## Table of Contents

- [Introduction](#introduction)
- [Dataset](#dataset)
- [Folder Structure](#folder-structure)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
- [Usage](#usage)
- [Dashboard](#dashboard)
- [TODO](#TODO)
- [Contact](#contact)

## Introduction

The Stack Exchange Data Insights project is designed to facilitate the extraction, processing, and analysis of data from the Stack Exchange network. By leveraging various tools such as Airflow, Docker, and DBT, users can seamlessly automate the data pipeline from downloading raw data to visualizing insights.

Understanding trends and patterns within Stack Exchange data can provide valuable insights into 150+ various topics, including technologies, finances, languages and many others. For me it was especially interesting to explore trends in the the fastest-emerging areas in the tech industry, such as AI and Data Science, where revolutionary technologies like Language Models (LLMs) have completely transformed our world recently. That's why in this project I analyzed questions and answers from [https://ai.stackexchange.com/](https://ai.stackexchange.com/) and [https://datascience.stackexchange.com/](https://datascience.stackexchange.com/) in order to explore trending topics and discussions in these fields and analyse how they changed over the years. 

With slightly modifications this project can be utilized to analyse the data from any other Stack Exchange site(s).

## Folder Structure

```
.
├── airflow/        # 
├── config/         # Configuration files
├── data/           # Data files
├── dbt/            # 
├── terraform       # 
├── .gitignore      # Git ignore rules
├── README.md       # This file
```

## Dataset

The Stack Exchange dataset is a collection of data from various [Stack Exchange]((https://stackexchange.com/)) sites, including Stack Overflow, Mathematics, Super User, and many others. It includes questions, answers, comments, tags, and other related data from these sites.

The dataset is updated regularly and can be accessed through the [Stack Exchange Data Explorer](https://data.stackexchange.com/). For this project, I used [Stack Exchange Data Dump]((https://archive.org/details/stackexchange)), which is hosted by the Internet Archive and is updated every three months.

For this project, I used data from the following Stack Exchange sites:

* [Data Science](https://datascience.stackexchange.com/)
* [Artificial Intelligence](https://ai.stackexchange.com/)
* [GenAI](https://genai.stackexchange.com/)

## Technologies

### TODO: Add description and pipleine picture

* Terraform
* Google Cloud Storage
* Google BigQuery
* Airflow - for data orcherstration
* Docker
* DBT Cloud
* Looker Studio

## Getting Started

### Prerequisites

Before you begin, ensure you have met the following requirements:

* GCP Project and Service Account
* Docker
* Terraform installed on your local machine

### Installation

Here are the detailed instructions on how to reproduce this project on your machine.

#### GCP Project and Credentials

#### Setup infrastructure with Terraform

#### Ingest data to BigQuery with Airflow

#### Perform data tranformations with DBT

### Usage

## Dashboard

![image1](./docs/dashboard1.png)

## TODO

* Use [Stack Exchange API](https://api.stackexchange.com/) instead of Stack Exchange Data Dump to get the most recent data.
* Create more DBT models and Looker Studio charts to provide more insights on the data.
* Add CI/CD.


