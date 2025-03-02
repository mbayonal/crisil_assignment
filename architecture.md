flowchart LR
    %% Data Source
    S1[(Storage System\nJSON Files)] --> |season-*.json| S2[Data Input Directory\n/data/input]
    
    subgraph "Kubernetes Cluster"
        subgraph "Docker Container"
            S2 --> |Mount| P1[/app/data/input]
            P1 --> |Read| E[Extract\nPySpark JSON Reader]
            
            E --> T1[Transform\nLeague Positions]
            E --> T2[Transform\nBest Scoring Team]
            
            T1 --> |Calculate points\nRank teams| L1[Load\nParquet Writer]
            T2 --> |Sum goals\nFind max| L2[Load\nParquet Writer]
            
            L1 --> P2[/app/data/output]
            L2 --> P2
            
            %% Runtime Environment
            R[Python Runtime\npython:3.13.2-slim] --- J[OpenJDK 11]
            J --- PS[PySpark]
            PS --- E
        end
    end
    
    P2 --> |Mount| S3[Data Output Directory\n/data/output]
    S3 --> |positions_{season}.parquet\nbest_scoring_team.parquet| S4[(Storage System\nParquet Files)]
    
    classDef storage fill:#f96,stroke:#333,stroke-width:2px
    classDef container fill:#bef,stroke:#333,stroke-width:2px
    classDef process fill:#bbf,stroke:#333,stroke-width:2px
    
    class S1,S4 storage
    class E,T1,T2,L1,L2 process
    class R,J,PS container
