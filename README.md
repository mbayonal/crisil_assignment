# **Premier League ETL**

##  **Project Description**
This project implements an **ETL pipeline** using **PySpark** to process Premier League match data stored in **JSON format**. The ETL extracts data, transforms it to calculate league positions and the best-scoring team per season, and stores the results in **Parquet format**.

The solution is containerized with **Docker** and designed to run on **Kubernetes** for scalability.

---

## **Architecture**

### **Premier League ETL Architecture**
```mermaid
%%{init: {'theme': 'default', 'flowchart': {'curve': 'basis', 'diagramPadding': 8, 'nodeSpacing': 30, 'rankSpacing': 40, 'padding': 10}}}%%
flowchart LR
    %% Data Source
    S1[(Storage\nJSON)] -->|season-*.json| S2[Input Dir\n/data/input]
    
    subgraph "K8s Cluster"
        subgraph "Docker Container" 
            S2 -->|Mount| P1[input]
            P1 -->|Read| E[Extract\nPySpark]
            
            E --> T1[Transform\nPositions]
            E --> T2[Transform\nBest Scoring]
            
            T1 -->|Calculate\nRank| L1[Load\nParquet]
            T2 -->|Sum\nMax| L2[Load\nParquet]
            
            L1 --> P2[output]
            L2 --> P2
            
            R[Python\n3.13.2-slim] --- J[JDK 11]
            J --- PS[PySpark]
            PS --- E
        end
    end
    
    P2 -->|Mount| S3[Output Dir\n/data/output]
    S3 -->|positions.parquet\nbest_scoring_team.parquet| S4[(Storage\nParquet)]
    
    classDef storage fill:#f96,stroke:#333,stroke-width:1.5px
    classDef container fill:#bef,stroke:#333,stroke-width:1.5px
    classDef process fill:#bbf,stroke:#333,stroke-width:1.5px
    
    class S1,S4 storage
    class E,T1,T2,L1,L2 process
    class R,J,PS container
```

---

##  **ETL Workflow**

### **Premier League ETL Workflow**
```mermaid
flowchart TD
    A[Initialize Spark Session] --> B[Receive input_path and output_path parameters]
    B --> C[Extraction: Read JSON files season-*.json]
    
    subgraph Extraction
        C --> D[Visualize initial DataFrame structure]
        D --> E[Extract season from filename]
        E --> F[Add 'season' column to DataFrame]
        F --> G[Validate updated structure]
    end
    
    G --> H[DataFrame with match data + season column]
    
    H --> I[Transformation: League Positions Calculation]
    H --> J[Transformation: Best Scoring Team Calculation]
    
    subgraph "Transformation: Positions"
        I --> I1[Calculate points for home and away teams]
        I1 --> I2[Group by team and season]
        I2 --> I3[Sum points and calculate statistics]
        I3 --> I4[Calculate goal difference]
        I4 --> I5[Assign position using rank]
    end
    
    subgraph "Transformation: Best Scoring Team"
        J --> J1[Group by team and season]
        J1 --> J2[Sum goals scored as home and away]
        J2 --> J3[Sort by total goals]
        J3 --> J4[Select team with highest value]
    end
    
    I5 --> K1[Save positions DataFrame in Parquet format]
    J4 --> K2[Save best scoring team DataFrame in Parquet format]
    
    K1 --> L[Partitioning by 'season']
    K2 --> M[Partitioning by 'season']
    
    L --> N[ETL process completion]
    M --> N
```

---

## 📖 **How to Deploy**

### **1 Prerequisites**
Ensure you have the following installed:
- **Docker** 
- **Kubernetes (kubectl & Minikube or a cluster)** 
- **Python 3.13.2** 

### **2 Build and Run with Docker**
#### **Build Docker Image:**
```bash
sudo docker build --no-cache -t etl_pyspark .
```

#### **Run Docker Container:**
```bash
sudo docker run --rm -v /home/user/data/input:/app/data/input \
                 -v /home/user/data/output:/app/data/output \
                 etl_pyspark
```

### **3 Deploy on Kubernetes**
#### **Create a Deployment (YAML):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etl-premier-league
spec:
  replicas: 1
  selector:
    matchLabels:
      app: etl-pyspark
  template:
    metadata:
      labels:
        app: etl-pyspark
    spec:
      containers:
      - name: etl-container
        image: etl_pyspark
        volumeMounts:
        - name: input-volume
          mountPath: /app/data/input
        - name: output-volume
          mountPath: /app/data/output
      volumes:
      - name: input-volume
        hostPath:
          path: /home/user/data/input
      - name: output-volume
        hostPath:
          path: /home/user/data/output
```

#### **Apply the Deployment:**
```bash
kubectl apply -f deployment.yaml
```

#### **Check Pod Status:**
```bash
kubectl get pods
```

#### **Check Logs:**
```bash
kubectl logs <POD_NAME>
```

### **4️⃣ Validate Output**
After execution, check the **output directory** for Parquet files:
```bash
ls /home/user/data/output
```
Expected output:
```
positions_0910.parquet  positions_1011.parquet  best_scoring_team.parquet
```

---

This ETL pipeline automates the processing of Premier League match data using **PySpark**. The **Docker + Kubernetes** integration makes the solution scalable and adaptable to production environments.


---

**Author:** Manuel Alejandro Bayona Leal  


---
