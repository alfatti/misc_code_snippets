import pandas as pd
import networkx as nx

# Sample DataFrame
df = pd.DataFrame({
    "TradeID": [1, 2, 3, 4, 5, 6, 7],
    "RelatedTradeID": [None, 1, 2, None, 6, 5, None]
})

# Step 1: Build the undirected graph
G = nx.Graph()

# Add nodes for all TradeIDs
G.add_nodes_from(df["TradeID"].dropna().unique())

# Add edges from TradeID ↔ RelatedTradeID pairs
for _, row in df.dropna(subset=["RelatedTradeID"]).iterrows():
    G.add_edge(row["TradeID"], row["RelatedTradeID"])

# Step 2: Extract connected components (clusters)
clusters = list(nx.connected_components(G))

# Optional: Convert each cluster to a sorted list
clusters = [sorted(list(c)) for c in clusters]

# Example output
print(clusters)
