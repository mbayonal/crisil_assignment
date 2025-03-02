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
