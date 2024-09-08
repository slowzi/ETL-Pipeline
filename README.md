
# Luigi Pipeline Project

## Problem

- The Sales team has sales data in a PostgreSQL database, but there are many missing values and the data format is not correct.

- The Product team has Electronic Product Pricing data in the form of .csv, but there are many missing values and the data format is messy

- The Data Scientist team does not have data for NLP (Natural Language Processing) modeling such as sentiment analysis and others.

## Solution
Transforming data owned by the sales team and product team to handle missing values, missmatches and inconsistent data. 

**disclaimer:*
`This scraping on trustpilot.com is for learning purposes only.`


## Dataset

 - [Amazon Sales Data Database](https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db)
 - [Electronic Product Pricing Data](https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view?usp=sharing)
 - [Amazon Services Review](https://www.trustpilot.com/review/www.amazon.com)

## Data Requirements
### sales table 

- fill null value = 0
- convert price column inr ke idr
- change price data type to float
- remove inconsistent value from rating
- change rating range from 0 - 5
- change no rating with most frequently values
- drop unesecery column
- save to csv

### product table
- column selection
- change column name
- convert price column usd to idr
- convert date column to datetime
- mapping availability column
- save to csv

### review table
- change date type data to datetime
- save to csv

## 🛠 Tech Stack
`Python, Luigi, Docker, PostgreSQL, DBeaver, BeautifulSoup`

save each night in csv and save each step to csv
save data every day at midnight, at each stage of the pipeline the data is saved in csv and save into postgresql data warehouse.



## Code Structure

Describe the structure of the codebase. Include details on the main directories and files, and their purpose.

```plaintext
project/
│
├── asset/                   # Image files 
│
├── data/                    # Storing data 
│   ├── raw                  
│   ├── validate               
│   ├── transform             
│   └── load                  
│
├── helper/                  # Func module
│
├── log/                     # Logging information
│
├── .gitignore               # Git ignore file
├── pipeline.py              # Pipeline code
├── run_pipeline.sh          # Run shell script
└── README.md                # This file
```

## Run project

Setup environtment
```Dotenv
# .env file example
DB_USERNAME = "yourusername"
DB_PASSWORD = "yourpassword"
DB_HOST = "yourhost"
DB_NAME = "yourdb"
```

Pull docker images
```docker
docker pull shandytp/amazon-sales-data-docker-db
```

Run docker images
```docker
docker run -d -p [AVAILABLE PORT]:5432 --name [CONTAINER_NAME] shandytp/amazon-sales-data-docker-db:latest

```
Create table scheme
```sql
CREATE TABLE IF NOT EXISTS yourdb (
    id SERIAL not null primary key,
    name VARCHAR NULL,
    main_category VARCHAR NULL,
    sub_category VARCHAR NULL,
    image VARCHAR NULL,
    link VARCHAR NULL,
    ratings INT NULL,
    no_of_ratings VARCHAR NULL,
    discount_price_idr DOUBLE PRECISION NULL,
    actual_price_idr DOUBLE PRECISION NULL
);

CREATE TABLE IF NOT EXISTS yourdb2 (
    id SERIAL not null primary key,    
    name VARCHAR NULL,
    brand VARCHAR NULL,
    categories VARCHAR NULL,
    currency VARCHAR NULL,
    max_price DOUBLE PRECISION NULL,
    min_price DOUBLE PRECISION NULL,
    availability VARCHAR NULL,
    urls VARCHAR NULL,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS yourdb3 (
    id SERIAL not null primary key,
	"user" VARCHAR NULL,
    location VARCHAR NULL,
    content TEXT NULL,
    rating INT NULL,
    experience_at TIMESTAMP NULL,
    scrape_at TIMESTAMP NULL
);
```

Run luigi visualizer (optional)
```sh
luigid --port 8082 > /dev/null 2> /dev/null &
```


Open crontab in wsl or unix
```sh
crontab -e
```

Create scheduler
```sh
0 0 * * * /home/ur path/run_pipeline.sh
```

Set run_pipeline.sh to your path and run this command for set permission that crontab can run.
```sh
chmod 755 run_pipeline.sh
```
## Testing:

Scenario 1 : ensure all data is properly loaded into the database
- sales db
![img_scr](/assets/sales.png)
- product db
![img_scr2](/assets/product.png)
- review db
![img_scr3](/assets/review.png)

Scenario 2 : insert data
```sql
INSERT INTO amazon_sales_data ("name",main_category,sub_category,image,link,ratings,no_of_ratings,discount_price,actual_price)
values ('Testing Product', 'Testing Category', 'Testing Sub Category', 'https://sekolahdata-assets.s3.ap-southeast-1.amazonaws.com/notebook-images/mde-intro-to-data-eng/testing_image.png', 'https://pacmann.io/', 5, 30, 450, 1000)
```

![img_scr4](/assets/insert_success.png)
![img_scr5](/assets/show_insert.png)


## Pipeline design

![pipeline](/assets/pipeline.png)
