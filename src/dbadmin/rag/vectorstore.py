"""ChromaDB-based vector store for RAG."""

from pathlib import Path
from typing import Any

import chromadb
from chromadb.config import Settings

from dbadmin.config import get_settings


class VectorStore:
    """ChromaDB vector store for database documentation.
    
    Stores embeddings of database documentation for RAG retrieval.
    Uses local persistence for offline use.
    """
    
    COLLECTION_NAME = "dbadmin_docs"
    
    def __init__(self, persist_dir: Path = None):
        """Initialize vector store.
        
        Args:
            persist_dir: Directory for persistent storage
        """
        settings = get_settings()
        self.persist_dir = persist_dir or settings.chroma_persist_dir
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB client with persistence
        self._client = chromadb.PersistentClient(
            path=str(self.persist_dir),
            settings=Settings(anonymized_telemetry=False),
        )
        
        # Get or create collection
        self._collection = self._client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            metadata={"description": "Database documentation for DbAdmin AI"},
        )
    
    def add_documents(
        self,
        documents: list[str],
        metadatas: list[dict[str, Any]] = None,
        ids: list[str] = None,
    ) -> None:
        """Add documents to the vector store.
        
        Args:
            documents: List of document texts
            metadatas: Optional metadata for each document
            ids: Optional unique IDs (auto-generated if not provided)
        """
        if not documents:
            return
        
        # Generate IDs if not provided
        if ids is None:
            existing_count = self._collection.count()
            ids = [f"doc_{existing_count + i}" for i in range(len(documents))]
        
        # Add to collection (ChromaDB handles embedding)
        self._collection.add(
            documents=documents,
            metadatas=metadatas or [{}] * len(documents),
            ids=ids,
        )
    
    def query(
        self,
        query_text: str,
        n_results: int = 5,
        where: dict = None,
    ) -> list[dict[str, Any]]:
        """Query the vector store for similar documents.
        
        Args:
            query_text: Query string
            n_results: Number of results to return
            where: Optional metadata filter
            
        Returns:
            List of matching documents with metadata and scores
        """
        results = self._collection.query(
            query_texts=[query_text],
            n_results=n_results,
            where=where,
        )
        
        # Format results
        documents = []
        if results["documents"] and results["documents"][0]:
            for i, doc in enumerate(results["documents"][0]):
                documents.append({
                    "content": doc,
                    "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                    "id": results["ids"][0][i] if results["ids"] else None,
                    "distance": results["distances"][0][i] if results["distances"] else None,
                })
        
        return documents
    
    def get_stats(self) -> dict[str, Any]:
        """Get vector store statistics."""
        return {
            "document_count": self._collection.count(),
            "collection_name": self.COLLECTION_NAME,
            "persist_dir": str(self.persist_dir),
        }
    
    def clear(self) -> None:
        """Clear all documents from the store."""
        self._client.delete_collection(self.COLLECTION_NAME)
        self._collection = self._client.create_collection(
            name=self.COLLECTION_NAME,
            metadata={"description": "Database documentation for DbAdmin AI"},
        )
