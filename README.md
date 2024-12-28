# 📦 AutoInventory
**A Fully Automated, Intelligent Warehouse Management System**  

---

## **Project Description**  
AutoInventory is an end-to-end, event-driven warehouse management solution that addresses real-world FMCG inventory challenges. The system integrates real-time data streaming, predictive analytics, and interactive dashboards to optimize inventory levels, prevent stockouts, and reduce waste—all with zero manual intervention.  

---

## **Dataset**  
This project uses a simulated FMCG dataset, enriched with:  
- **Sales Metrics:** Sales volume, price, and promotion data.  
- **Inventory Metrics:** Stock levels and replenishment lead times.  
- **Temporal Components:** Date, weekday, and month.  
- **Geospatial Information:** Store locations and product categories.  
- **Dataset Link:** [FMCG Sales Demand Forecasting Dataset on Kaggle](https://www.kaggle.com/datasets/krishanukalita/fmcg-sales-demand-forecasting-and-optimization/data)  

---

## **Architecture and Technologies Used**  
### **Architecture**  
1. **Data Streaming:** Kafka (running in Docker) streams inventory data into S3.  
2. **ETL Pipeline:** Glue processes data, and Lambda triggers transformations dynamically.  
3. **Data Warehousing:** Redshift stores analytical-ready data.  
4. **Dashboards:** Streamlit and Power BI provide interactive visualizations. 
![image](https://github.com/user-attachments/assets/987af82b-4825-42f5-97f5-9b022a31140f)

---

## **Project Implementation**  
### **Step 1: Real-Time Data Streaming**  
- **Technology Used:** Apache Kafka (deployed with Docker)  
- Simulates real-time inventory updates by streaming data from sales points to AWS S3 as a centralized data lake.  
- **Key Features:**  
  - Ensures idempotency with custom logic.  
  - Handles hourly batching for efficient ingestion.

### **Step 2: ETL Pipeline**  
- **Technologies Used:** AWS Glue and Lambda  
- **ETL Highlights:**  
  - Extracts raw data from S3 and applies transformations like revenue, cost, and profit margin calculations.  
  - Processes and loads transformed data into AWS Redshift for analytical queries.  
- **Event-Driven:** Automatically triggered by S3 file uploads using Lambda.  

### **Step 3: Predictive Analytics**  
- **Technology Used:** PyTorch custom Neural Network Models  
- Forecasts sales and stock levels 5–7 days into the future.  
- **Key Features:**  
  - Rolling forecasts for stability.  
  - Advanced preprocessing with outlier resistance and missing data handling.  
  - Automatic model retraining for evolving data patterns.  

### **Step 4: Dashboards and Insights**  
- **Technologies Used:** Streamlit and Power BI  
- **Streamlit:** Provides forecasting, historical data line graphs, and basic insights in real time.
![Screenshot 2024-12-27 181718](https://github.com/user-attachments/assets/fe7fa640-dc26-45ca-859a-f24912ace7a1)

- **Power BI:** Offers interactive dashboards with drill-down capabilities by store location and product category, and advanced visuals for current stock levels, replenishment times, and revenue trends.  
![warehouse_page-0001 (1)](https://github.com/user-attachments/assets/ed0e81f6-2601-43e3-a7e9-b1678532252e)
--- 

### **Technologies**  
- **Data Streaming:** Apache Kafka (Dockerized)  
- **AWS S3:** Acts as a centralized data lake for raw and processed data.
- **AWS Lambda:** For triggering ETL processes dynamically based on S3 events.
- **ETL:** 
  - **AWS Glue:** For scalable data transformation and data loading to AWS Redshift.  
- **Data Warehousing:** AWS Redshift  
- **Machine Learning:** PyTorch (custom neural network models for time-series forecasting)  
- **Dashboards:** Streamlit and Power BI  
- **Cloud Management:** AWS IAM for permissions and AWS Secrets Manager for secure credential handling  
- **Experiment Tracking:** MLflow  
- **Database Interaction:** SQLAlchemy  
- **Containerization:** Docker  

---

## **Business Impact**  
- **Prevented Stockouts:** Predictive analytics keep shelves stocked with high-demand products.  
- **Reduced Waste:** Optimized inventory minimizes spoilage and overstock.  
- **Improved Decision-Making:** Automated KPIs like profit margins and replenishment times enable smarter choices.  

---

## **Connect with Me**  
Have questions or want to collaborate? Let’s connect!  
- **LinkedIn:** [Profile](https://www.linkedin.com/in/el-meziane-cha%C3%AFma/)  
