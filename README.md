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

The Stack Exchange Data Insights project is designed to facilitate the extraction, processing, and analysis of data from the Stack Exchange network. By leveraging various tools such as Airflow, Docker and DBT, users can seamlessly automate the data pipeline from downloading raw data to visualizing insights.

Understanding trends and patterns within Stack Exchange data can provide valuable insights into 150+ various topics, including technologies, finances, languages and many others. For me it was especially interesting to explore trends in the the fastest-emerging areas in the tech industry, such as AI and Data Science, where revolutionary technologies like Language Models (LLMs) have completely transformed our world recently. That's why in this project I analyzed questions and answers from [https://ai.stackexchange.com/](https://ai.stackexchange.com/) and [https://datascience.stackexchange.com/](https://datascience.stackexchange.com/) in order to explore trending topics and discussions in these fields and analyse how they changed over the years.

Here are ... TODO: Provide examples 

With slightly modifications this project can be utilized to analyse the data from any other Stack Exchange site(s).

## Folder Structure

```
.
├── airflow/        # 
├── config/         # Configuration files
├── data/           # Data files
├── dbt/            # 
├── docs/           # 
├── terraform       # 
├── .gitignore      # Git ignore rules
├── README.md       # This file
```

## Dataset

The Stack Exchange dataset is a collection of data from various [Stack Exchange]((https://stackexchange.com/)) sites, including Stack Overflow, Mathematics, Super User, and many others. It includes questions, answers, comments, tags, and other related data from these sites.

The dataset is updated regularly and can be accessed through the [Stack Exchange Data Explorer](https://data.stackexchange.com/). For this project, I used [Stack Exchange Data Dump]((https://archive.org/details/stackexchange)), which is hosted by the Internet Archive and is updated every three months.

For this project, I used data from the following Stack Exchange sites:

* [Data Science](https://ia904700.us.archive.org/view_archive.php?archive=/6/items/stackexchange/datascience.stackexchange.com.7z)
* [Artificial Intelligence](https://ia804700.us.archive.org/view_archive.php?archive=/6/items/stackexchange/ai.stackexchange.com.7z)
* [GenAI](https://ia904700.us.archive.org/view_archive.php?archive=/6/items/stackexchange/genai.stackexchange.com.7z)

## Technologies

### TODO: Add description and pipleine picture

* **Terraform** - IaC tool
* **Google Cloud Storage** - Data Lake
* **Google BigQuery** - Data Warehouse
* **Airflow** - Data Orchestration tool
* **Docker** - Containerization tool
* **DBT Cloud** - Data Transformations and Modeling
* **Looker Studio** - Data Visualisation

## Getting Started

### Prerequisites

Before you begin, ensure you have met the following requirements:

* Python 3
* Google Cloud SDK ([installation](https://cloud.google.com/sdk/docs/install-sdk))
* GCP Project ([Initial Setup](https://github.com/dianagromakovskaya/data-engineering-zoomcamp/blob/main/01-docker-terraform/1_terraform_gcp/2_gcp_overview.md#initial-setup))
* GCP Service Account with Storage Admin, BigQuery Admin and Compute Admin roles ([Setup for Access](https://github.com/dianagromakovskaya/data-engineering-zoomcamp/blob/main/01-docker-terraform/1_terraform_gcp/2_gcp_overview.md#setup-for-access))
* Docker and Docker Compose ([installation](https://docs.docker.com/compose/install/))
* Terraform ([installation](https://www.terraform.io/downloads))

### Project Installation and Setup

Below you can find the detailed instructions on how to set up this project.

#### 1. Clone the Repository

```bash
git clone https://github.com/dianagromakovskaya/stackexchange-data-insights.git
cd stackexchange-data-insights
```

#### 2. Configure GCP Project and Credentials

Replace GCP_PROJECT_ID in [config/config.yaml](config/config.yaml) file with your GCP project id and put your Service Account credentials to the [credentials/google_credentials.json](credentials/google_credentials.json) file. NOTE: this file path ([credentials/google_credentials.json](credentials/google_credentials.json)) is used to configure GCP connection in Terraform and Airflow, so please don't change the name and location of this file.

You can also change the bucket/dataset name to something else instead of default names via bucket and dataset fields in [config/config.yaml](config/config.yaml) file.

#### 3. Setup infrastructure with Terraform

* Check that Terraform is installed

```bash
terraform version
```

* Initialize the project

```bash
cd terraform
terraform init
```

* Check Execution Plan

Put your GCP project id instead of {GCP_PROJECT_ID}. If you specified your own bucket and dataset names in [config/config.yaml](config/config.yaml), remember to specify them using the `gcs_bucket` and `bq_dataset_name` variables.

```bash
terraform plan -var 'project={GCP_PROJECT_ID}'
```

After execution of this command, you should see something like that:
```bash
Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
...
  # google_storage_bucket.gcs-bucket will be created
Plan: 2 to add, 0 to change, 0 to destroy.
...

```

* Apply changes from the propose Execution Plan to cloud

Put your GCP project id instead of {GCP_PROJECT_ID}. If you specified your own bucket and dataset names in [config/config.yaml](config/config.yaml), remember to specify them using the `gcs_bucket` and `bq_dataset_name` variables.
```bash
terraform apply -var 'project={GCP_PROJECT_ID}'
```

After execution of this command, you should see:

```bash
Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

* Ensure that GCP resources have been created

#### 4. Ingest data to BigQuery with Airflow

```bash
cd airflow
docker compose build
docker compose up -d
```

Navigate to 

```bash
docker compose down --volumes --remove-orphans
```

#### 5. Perform data tranformations with dbt Cloud

#### 6. Visualise data with Looker Studio


## Dashboard

![image1](./docs/dashboard1.png)

## TODO

* Use [Stack Exchange API](https://api.stackexchange.com/) instead of Stack Exchange Data Dump to get the most recent data.
* Add more DBT models and Looker Studio charts to provide more insights on the data.
* Add CI/CD.
* Add tests, logging and excpetions handlers.

## Contact

If you have any questions or suggestions, feel free to connect with me on [Linkedin](https://www.linkedin.com/in/diana-gromakovskaya-49931b188/).


