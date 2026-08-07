import json
from typing import List, Optional

from fastapi import FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="VectorShift Pipeline Backend")

# Allow CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/')
def read_root():
    return {'Ping': 'Pong'}


def is_directed_acyclic_graph(node_ids: List[str], edges: List[tuple]) -> bool:
    """Kahn's algorithm: repeatedly remove nodes with in-degree 0.
    If all nodes can be removed, the graph contains no cycles and is a DAG.
    """
    if not node_ids:
        return True

    node_set = set(node_ids)
    in_degree = {node_id: 0 for node_id in node_set}
    adjacency = {node_id: [] for node_id in node_set}

    for source, target in edges:
        if source in node_set and target in node_set:
            adjacency[source].append(target)
            in_degree[target] += 1

    # Initialize queue with all nodes that have in-degree 0
    queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
    visited_count = 0

    while queue:
        current = queue.pop(0)
        visited_count += 1
        for neighbor in adjacency[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return visited_count == len(node_set)


@app.post('/pipelines/parse')
async def parse_pipeline(request: Request, pipeline: Optional[str] = Form(None)):
    nodes = []
    edges = []

    if pipeline:
        try:
            parsed = json.loads(pipeline)
            nodes = parsed.get('nodes', []) or []
            edges = parsed.get('edges', []) or []
        except Exception:
            pass
    else:
        try:
            body = await request.json()
            nodes = body.get('nodes', []) or []
            edges = body.get('edges', []) or []
        except Exception:
            pass

    node_ids = [str(node.get('id')) for node in nodes if node.get('id') is not None]
    unique_node_ids = list(set(node_ids))

    edge_pairs = [
        (str(edge.get('source')), str(edge.get('target')))
        for edge in edges
        if edge.get('source') is not None and edge.get('target') is not None
    ]

    is_dag = is_directed_acyclic_graph(unique_node_ids, edge_pairs)

    return {
        'num_nodes': len(nodes),
        'num_edges': len(edges),
        'is_dag': is_dag,
    }

