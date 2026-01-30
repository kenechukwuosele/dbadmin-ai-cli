"""Document retriever with ChromaDB for RAG."""

from typing import Any

from dbadmin.rag.vectorstore import VectorStore


class DocumentRetriever:
    """Retriever for database documentation.
    
    Uses ChromaDB vector store to find relevant documentation
    for user queries about database administration.
    """
    
    def __init__(self):
        """Initialize retriever with vector store."""
        self._store = VectorStore()
    
    def retrieve(
        self,
        query: str,
        k: int = 5,
        db_type: str = None,
    ) -> list[dict[str, Any]]:
        """Retrieve relevant documents for a query.
        
        Args:
            query: User query
            k: Number of documents to retrieve
            db_type: Optional filter for database type
            
        Returns:
            List of relevant document chunks with source info
        """
        where = None
        if db_type:
            where = {"db_type": db_type}
        
        results = self._store.query(query, n_results=k, where=where)
        
        # Add source info
        for result in results:
            metadata = result.get("metadata", {})
            result["source"] = metadata.get("source", "Database documentation")
        
        return results
    
    def add_documentation(
        self,
        content: str,
        source: str,
        db_type: str,
        chunk_size: int = 500,
    ) -> int:
        """Add documentation to the retriever.
        
        Args:
            content: Documentation text
            source: Source name (e.g., "PostgreSQL Documentation")
            db_type: Database type (postgresql, mysql, mongodb, redis)
            chunk_size: Size of text chunks
            
        Returns:
            Number of chunks added
        """
        # Split into chunks
        chunks = self._chunk_text(content, chunk_size)
        
        # Create metadata for each chunk
        metadatas = [
            {"source": source, "db_type": db_type, "chunk_index": i}
            for i in range(len(chunks))
        ]
        
        # Add to vector store
        self._store.add_documents(chunks, metadatas)
        
        return len(chunks)
    
    def _chunk_text(self, text: str, chunk_size: int) -> list[str]:
        """Split text into overlapping chunks."""
        chunks = []
        overlap = chunk_size // 4
        
        sentences = text.replace('\n', ' ').split('. ')
        current_chunk = ""
        
        for sentence in sentences:
            if len(current_chunk) + len(sentence) < chunk_size:
                current_chunk += sentence + ". "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                # Start new chunk with overlap
                words = current_chunk.split()
                overlap_text = " ".join(words[-overlap//5:]) if len(words) > overlap//5 else ""
                current_chunk = overlap_text + " " + sentence + ". "
        
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def get_stats(self) -> dict[str, Any]:
        """Get retriever statistics."""
        return self._store.get_stats()
    
    def is_initialized(self) -> bool:
        """Check if documentation has been ingested."""
        stats = self.get_stats()
        return stats.get("document_count", 0) > 0
