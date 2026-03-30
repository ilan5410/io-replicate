# Deterministic nodes — no external deps beyond numpy/pandas
from .model_builder import model_builder_node
from .decomposer import decomposer_node

# Agentic nodes — require langchain_core / langgraph
try:
    from .paper_analyst import paper_analyst_node
    from .data_acquirer import data_acquirer_node
    from .data_preparer import data_preparer_node
    from .output_producer import output_producer_node
    from .reviewer import reviewer_node
    _agentic_available = True
except ImportError:
    _agentic_available = False

__all__ = [
    "model_builder_node",
    "decomposer_node",
    "paper_analyst_node",
    "data_acquirer_node",
    "data_preparer_node",
    "output_producer_node",
    "reviewer_node",
]
