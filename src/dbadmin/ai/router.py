"""Intelligent task routing and critic pattern for AI operations."""

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

from dbadmin.ai.llm import LLMClient, PROVIDERS


class TaskType(str, Enum):
    """Types of tasks for routing."""
    # Simple tasks -> fast/cheap model
    INTENT_CLASSIFICATION = "intent"
    SIMPLE_EXTRACTION = "extraction"
    SUMMARIZATION = "summary"
    
    # Complex tasks -> smart model
    SQL_GENERATION = "sql"
    QUERY_OPTIMIZATION = "optimization"
    SCHEMA_ANALYSIS = "schema"
    ARCHITECTURAL = "architecture"
    REASONING = "reasoning"
    
    # Default
    GENERAL = "general"


@dataclass
class ModelTier:
    """Model configuration for a tier."""
    provider: str
    model: str
    description: str


# Model tiers for routing
MODEL_TIERS = {
    # Fast/cheap for simple tasks (80% of requests)
    "mini": [
        ModelTier("groq", "llama-3.1-8b-instant", "Fastest, free"),
        ModelTier("openrouter", "meta-llama/llama-3.1-8b-instruct:free", "Free"),
        ModelTier("openai", "gpt-4o-mini", "Fast, cheap"),
        ModelTier("ollama", "llama3.1", "Local, free"),
    ],
    # Smart for complex tasks (20% of requests)
    "smart": [
        ModelTier("openrouter", "anthropic/claude-3.5-sonnet", "Best reasoning"),
        ModelTier("openai", "gpt-4o", "Strong all-around"),
        ModelTier("groq", "llama-3.1-70b-versatile", "Fast, capable"),
        ModelTier("anthropic", "claude-3-5-sonnet-20241022", "Best for code"),
    ],
    # Reasoning specialists (for critic/verification)
    "reasoning": [
        ModelTier("openai", "o1-mini", "Specialized reasoning"),
        ModelTier("openrouter", "anthropic/claude-3.5-sonnet", "Strong analysis"),
        ModelTier("deepseek", "deepseek-chat", "Good reasoning, cheap"),
    ],
}

# Task type to tier mapping
TASK_ROUTING = {
    TaskType.INTENT_CLASSIFICATION: "mini",
    TaskType.SIMPLE_EXTRACTION: "mini",
    TaskType.SUMMARIZATION: "mini",
    TaskType.SQL_GENERATION: "smart",
    TaskType.QUERY_OPTIMIZATION: "smart",
    TaskType.SCHEMA_ANALYSIS: "smart",
    TaskType.ARCHITECTURAL: "reasoning",
    TaskType.REASONING: "reasoning",
    TaskType.GENERAL: "mini",  # Default to cheap
}


class TaskRouter:
    """Routes tasks to appropriate models based on complexity.
    
    Strategy:
    - 80% of simple tasks -> mini models (fast, cheap)
    - 20% of complex tasks -> smart models (capable)
    
    This reduces API costs by ~90% while maintaining quality.
    """
    
    def __init__(self):
        # Cache available models per tier
        self._available = {}
        for tier, models in MODEL_TIERS.items():
            for model_tier in models:
                if self._is_available(model_tier):
                    self._available[tier] = model_tier
                    break
    
    def _is_available(self, model: ModelTier) -> bool:
        """Check if a model is available (has API key or is local)."""
        config = PROVIDERS.get(model.provider, {})
        env_key = config.get("env_key")
        
        if env_key:
            return bool(os.getenv(env_key))
        elif model.provider == "ollama":
            try:
                import httpx
                return httpx.get("http://localhost:11434/api/tags", timeout=0.5).status_code == 200
            except Exception:
                return False
        return False
    
    def classify_task(self, user_input: str) -> TaskType:
        """Classify user input into a task type.
        
        Uses heuristics for fast classification without LLM call.
        """
        input_lower = user_input.lower()
        
        # SQL generation patterns
        sql_keywords = ["show", "find", "get", "list", "count", "select", "query", "fetch"]
        if any(kw in input_lower for kw in sql_keywords):
            # Check if it's complex
            complex_keywords = ["join", "optimize", "group by", "subquery", "aggregate"]
            if any(kw in input_lower for kw in complex_keywords):
                return TaskType.SQL_GENERATION
            return TaskType.SIMPLE_EXTRACTION
        
        # Optimization patterns
        if any(kw in input_lower for kw in ["optimize", "slow", "performance", "index", "speed"]):
            return TaskType.QUERY_OPTIMIZATION
        
        # Schema/architecture patterns  
        if any(kw in input_lower for kw in ["schema", "design", "structure", "model", "architecture"]):
            return TaskType.SCHEMA_ANALYSIS
        
        # Intent/help patterns
        if any(kw in input_lower for kw in ["help", "how", "what is", "explain", "why"]):
            return TaskType.SUMMARIZATION
        
        return TaskType.GENERAL
    
    def get_model_for_task(self, task_type: TaskType) -> tuple[str, str]:
        """Get best available model for a task type.
        
        Returns:
            (provider, model) tuple
        """
        tier = TASK_ROUTING.get(task_type, "mini")
        
        if tier in self._available:
            model = self._available[tier]
            return model.provider, model.model
        
        # Fallback to any available
        for t in ["mini", "smart", "reasoning"]:
            if t in self._available:
                model = self._available[t]
                return model.provider, model.model
        
        # Last resort
        return "openrouter", "meta-llama/llama-3.1-8b-instruct:free"
    
    def get_client_for_task(self, task_type: TaskType) -> LLMClient:
        """Get an LLM client configured for the task type."""
        provider, model = self.get_model_for_task(task_type)
        return LLMClient(provider=provider, model=model)


@dataclass
class CriticResult:
    """Result from critic review."""
    is_valid: bool
    issues: list[str]
    suggestions: list[str]
    confidence: float  # 0-1
    

class CriticPattern:
    """Self-correction using two different models.
    
    Pattern:
    1. Generator model creates initial output
    2. Critic model (different provider) reviews for errors
    3. If issues found, regenerate with critic feedback
    
    This catches "blind spots" that a single model might have.
    """
    
    def __init__(self, max_iterations: int = 2):
        self.max_iterations = max_iterations
        self.router = TaskRouter()
    
    def _get_critic_model(self, generator_provider: str) -> tuple[str, str]:
        """Get a critic model different from the generator."""
        # Prefer different provider to avoid same blind spots
        available = []
        for tier in ["reasoning", "smart"]:
            if tier in self.router._available:
                model = self.router._available[tier]
                if model.provider != generator_provider:
                    available.append((model.provider, model.model))
        
        if available:
            return available[0]
        
        # Fallback to same provider, different model
        return self.router.get_model_for_task(TaskType.REASONING)
    
    def generate_with_review(
        self,
        prompt: str,
        task_type: TaskType = TaskType.SQL_GENERATION,
        context: dict = None,
    ) -> dict[str, Any]:
        """Generate content with critic review.
        
        Args:
            prompt: The generation prompt
            task_type: Type of task for routing
            context: Additional context (schema, etc.)
            
        Returns:
            dict with 'content', 'reviewed', 'iterations', 'issues_found'
        """
        # Get generator
        gen_provider, gen_model = self.router.get_model_for_task(task_type)
        generator = LLMClient(provider=gen_provider, model=gen_model)
        
        # Get critic (different provider)
        critic_provider, critic_model = self._get_critic_model(gen_provider)
        critic = LLMClient(provider=critic_provider, model=critic_model)
        
        result = {
            "content": "",
            "reviewed": False,
            "iterations": 0,
            "issues_found": [],
            "generator": f"{gen_provider}/{gen_model}",
            "critic": f"{critic_provider}/{critic_model}",
        }
        
        current_prompt = prompt
        
        for i in range(self.max_iterations):
            result["iterations"] = i + 1
            
            # Generate
            gen_messages = [{"role": "user", "content": current_prompt}]
            gen_response = generator.complete(gen_messages, temperature=0.3)
            result["content"] = gen_response.content
            
            # Critic review
            critic_review = self._review_output(
                critic, 
                prompt, 
                gen_response.content,
                task_type,
                context,
            )
            
            if critic_review.is_valid and critic_review.confidence > 0.8:
                result["reviewed"] = True
                break
            
            # Record issues
            result["issues_found"].extend(critic_review.issues)
            
            # Regenerate with feedback
            feedback = "\n".join([
                f"Previous output had issues:",
                *[f"- {issue}" for issue in critic_review.issues],
                "",
                "Please fix these issues:",
                *[f"- {sug}" for sug in critic_review.suggestions],
            ])
            current_prompt = f"{prompt}\n\n{feedback}"
        
        return result
    
    def _review_output(
        self,
        critic: LLMClient,
        original_prompt: str,
        output: str,
        task_type: TaskType,
        context: dict = None,
    ) -> CriticResult:
        """Have critic model review the output."""
        review_prompt = f"""Review this output for errors, issues, or improvements.

ORIGINAL REQUEST:
{original_prompt}

OUTPUT TO REVIEW:
{output}

{f"CONTEXT: {context}" if context else ""}

Analyze for:
1. Correctness - Is the output accurate?
2. Completeness - Does it fully address the request?
3. Safety - Any dangerous operations (for SQL: injections, data loss)?
4. Best practices - Does it follow conventions?

Respond in this exact format:
VALID: [YES/NO]
CONFIDENCE: [0.0-1.0]
ISSUES:
- [issue 1]
- [issue 2]
SUGGESTIONS:
- [suggestion 1]
- [suggestion 2]
"""
        
        messages = [{"role": "user", "content": review_prompt}]
        response = critic.complete(messages, temperature=0.1)
        
        # Parse response
        return self._parse_critic_response(response.content)
    
    def _parse_critic_response(self, response: str) -> CriticResult:
        """Parse critic's structured response."""
        is_valid = "VALID: YES" in response.upper()
        
        # Extract confidence
        confidence = 0.5
        if "CONFIDENCE:" in response.upper():
            try:
                import re
                match = re.search(r'CONFIDENCE:\s*([\d.]+)', response, re.I)
                if match:
                    confidence = float(match.group(1))
            except Exception:
                pass
        
        # Extract issues
        issues = []
        in_issues = False
        for line in response.split("\n"):
            if "ISSUES:" in line.upper():
                in_issues = True
                continue
            if "SUGGESTIONS:" in line.upper():
                in_issues = False
            if in_issues and line.strip().startswith("-"):
                issues.append(line.strip()[1:].strip())
        
        # Extract suggestions
        suggestions = []
        in_suggestions = False
        for line in response.split("\n"):
            if "SUGGESTIONS:" in line.upper():
                in_suggestions = True
                continue
            if in_suggestions and line.strip().startswith("-"):
                suggestions.append(line.strip()[1:].strip())
        
        return CriticResult(
            is_valid=is_valid,
            issues=issues,
            suggestions=suggestions,
            confidence=confidence,
        )


# Convenience functions
def get_router() -> TaskRouter:
    """Get a task router instance."""
    return TaskRouter()


def generate_with_critic(
    prompt: str,
    task_type: TaskType = TaskType.SQL_GENERATION,
) -> dict:
    """Generate content with critic review."""
    critic = CriticPattern()
    return critic.generate_with_review(prompt, task_type)
