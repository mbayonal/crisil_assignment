# Premier League ETL Workflow

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
