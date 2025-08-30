"""
Enhanced LLM-driven Agent for Version 2
Combines the existing task architecture with LLM intelligence
"""

import asyncio
import json
import re
import logging
from typing import Any, Dict, List, Union, Optional
from datetime import datetime

try:
    import openai
except ImportError:
    openai = None

from config import get_settings
from utils.formats import parse_questions, parse_plan
from utils.llm_client import ask_openai_json
from tasks import sales, network, weather, wikipedia, highcourt, duckdb_tasks, generic
from chart_generator import ChartGenerator

logger = logging.getLogger(__name__)

class EnhancedLLMAgent:
    """
    Enhanced agent that combines existing task modules with LLM-driven analysis
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.chart_generator = ChartGenerator()
        if openai and self.settings.openai_api_key:
            self.openai_client = openai.AsyncOpenAI(api_key=self.settings.openai_api_key)
        else:
            self.openai_client = None
    
    async def analyze(self, question: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced analysis using LLM + existing task modules
        """
        try:
            logger.info(f"Starting enhanced analysis for: {question[:100]}...")
            
            # Extract required JSON structure from question
            required_structure = self._extract_json_structure(question)
            logger.info(f"Detected required structure: {list(required_structure.keys())}")
            
            # Use LLM to determine best analysis approach
            analysis_plan = await self._create_analysis_plan(question, inputs, required_structure)
            
            # Execute analysis based on plan
            if analysis_plan.get("use_llm_primary", False):
                # Pure LLM approach for complex/custom analysis
                result = await self._llm_primary_analysis(question, inputs, required_structure)
            else:
                # Hybrid: Use existing task + LLM enhancement
                result = await self._hybrid_analysis(question, inputs, required_structure, analysis_plan)
            
            # Ensure result matches required structure
            validated_result = self._validate_and_fix_structure(result, required_structure)
            
            logger.info("Enhanced analysis completed successfully")
            return validated_result
                
        except Exception as e:
            logger.error(f"Enhanced analysis failed: {e}", exc_info=True)
            return self._create_fallback_response(question)
    
    def _extract_json_structure(self, question: str) -> Dict[str, Any]:
        """
        Extract the expected JSON structure from the question text
        Similar to Version 1 approach
        """
        structure = {}
        
        # Look for bullet points with field names
        bullet_pattern = r'[-*]\s*([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(bullet_pattern, question)
        
        for field in matches:
            # Determine field type based on name patterns
            field_lower = field.lower()
            
            if any(keyword in field_lower for keyword in ['chart', 'plot', 'graph', 'image']):
                structure[field] = "data:image/png;base64,"
            elif any(keyword in field_lower for keyword in ['correlation', 'ratio', 'rate', 'percentage']):
                structure[field] = 0.0
            elif any(keyword in field_lower for keyword in ['total', 'count', 'sum', 'number', 'median', 'avg']):
                structure[field] = 0
            elif any(keyword in field_lower for keyword in ['top', 'best', 'name', 'title', 'region', 'court']):
                structure[field] = ""
            elif 'tax' in field_lower:
                structure[field] = 0.0
            else:
                structure[field] = ""
        
        # If no structure found, try to infer from question content
        if not structure:
            if 'sales' in question.lower():
                structure = {
                    "total_sales": 0,
                    "top_region": "",
                    "analysis_result": ""
                }
            elif 'network' in question.lower() or 'latency' in question.lower():
                structure = {
                    "avg_latency_ms": 0.0,
                    "analysis_result": ""
                }
            elif 'court' in question.lower():
                structure = {
                    "top_court": "",
                    "analysis_result": ""
                }
            else:
                structure = {"analysis_result": "", "data_summary": ""}
        
        return structure
    
    async def _create_analysis_plan(self, question: str, inputs: Dict[str, Any], 
                                  required_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM to create an intelligent analysis plan
        """
        if not self.openai_client:
            return {"use_existing_tasks": True, "primary_task": "generic"}
        
        try:
            system_prompt = """You are an expert data analysis planner. Based on the question and required output structure, determine the best analysis approach.

Return JSON with:
- use_llm_primary: true if question is complex/custom and needs full LLM analysis
- use_existing_tasks: true if existing task modules can handle it
- primary_task: "sales", "network", "weather", "wikipedia", "highcourt", "duckdb", or "generic"
- enhancement_needed: true if existing task output needs LLM enhancement
- reasoning: brief explanation

Consider:
- Simple structured questions with clear patterns → use existing tasks
- Complex custom analysis → use LLM primary
- Standard tasks but complex output structure → use hybrid approach"""

            user_message = f"""
QUESTION: {question}

REQUIRED OUTPUT STRUCTURE: {json.dumps(required_structure, indent=2)}

AVAILABLE DATA: {json.dumps(inputs, indent=2, default=str)}

Determine the best analysis approach."""

            response = await self.openai_client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=500,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            plan = json.loads(response.choices[0].message.content)
            logger.info(f"Analysis plan: {plan.get('reasoning', 'No reasoning provided')}")
            return plan
            
        except Exception as e:
            logger.error(f"Failed to create analysis plan: {e}")
            return {"use_existing_tasks": True, "primary_task": "generic"}
    
    async def _llm_primary_analysis(self, question: str, inputs: Dict[str, Any], 
                                   required_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM as primary analysis engine (similar to Version 1)
        """
        if not self.openai_client:
            return self._create_fallback_response(question)
        
        try:
            system_prompt = f"""You are an expert data analyst. Analyze the provided data and return EXACTLY the JSON structure requested.

CRITICAL REQUIREMENTS:
1. You MUST return a valid JSON object with exactly these fields: {list(required_structure.keys())}
2. Each field must have the correct data type as specified
3. For image fields (containing 'chart', 'plot', 'graph'), return "data:image/png;base64," (empty placeholder)
4. For numeric fields, return actual calculated numbers (integers or floats as appropriate)
5. For string fields, return meaningful text values
6. Do NOT return placeholder values like 0, "", or null unless the data actually results in those values

EXPECTED OUTPUT STRUCTURE:
{json.dumps(required_structure, indent=2)}

ANALYSIS GUIDELINES:
1. If data is provided, perform actual calculations and analysis
2. For correlations, use statistical methods (Pearson correlation)
3. For aggregations (total, sum, median), calculate from actual data
4. For "top" fields, find the actual highest/best value
5. For tax calculations, apply the specified percentage (e.g., 10% = multiply by 0.10)
6. For charts/plots: return empty data URI placeholder for now

Remember: Your output will be directly parsed as JSON, so ensure it's valid JSON format."""

            user_message = f"""
ANALYSIS REQUEST: {question}

AVAILABLE DATA:
{json.dumps(inputs, indent=2, default=str)}

Please perform the analysis and return the exact JSON structure with calculated values.
"""

            response = await self.openai_client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=self.settings.llm_max_tokens,
                temperature=self.settings.llm_temperature,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            logger.info(f"LLM primary analysis completed: {list(result.keys())}")
            return result
            
        except Exception as e:
            logger.error(f"LLM primary analysis failed: {e}")
            return self._create_fallback_response(question)
    
    async def _hybrid_analysis(self, question: str, inputs: Dict[str, Any], 
                              required_structure: Dict[str, Any], 
                              analysis_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Hybrid approach: Use existing task modules + LLM enhancement
        """
        try:
            # Run existing task
            task_name = analysis_plan.get("primary_task", "generic")
            task_result = await self._run_existing_task(task_name, question, inputs)
            
            # Enhance with LLM if needed
            if analysis_plan.get("enhancement_needed", False) and self.openai_client:
                enhanced_result = await self._enhance_with_llm(
                    question, task_result, required_structure
                )
                return enhanced_result
            else:
                # Map task result to required structure
                return self._map_task_result_to_structure(task_result, required_structure)
                
        except Exception as e:
            logger.error(f"Hybrid analysis failed: {e}")
            return self._create_fallback_response(question)
    
    async def _run_existing_task(self, task_name: str, question: str, 
                                inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the appropriate existing task module
        """
        try:
            dfs = inputs.get("dfs", [])
            
            if task_name == "sales":
                return sales.run_sales(question, {"dfs": dfs})
            elif task_name == "network":
                return network.run_network(question, {"dfs": dfs})
            elif task_name == "weather":
                return weather.run_weather(question, {"dfs": dfs})
            elif task_name == "wikipedia":
                return wikipedia.run_wikipedia(question)
            elif task_name == "highcourt":
                return highcourt.run_highcourt(question)
            elif task_name == "duckdb":
                return duckdb_tasks.run_duckdb_example(question)
            else:
                return generic.run_generic(question, {"dfs": dfs})
                
        except Exception as e:
            logger.error(f"Task {task_name} failed: {e}")
            return {"error": f"Task {task_name} failed: {str(e)}"}
    
    async def _enhance_with_llm(self, question: str, task_result: Dict[str, Any], 
                               required_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM to enhance existing task results to match required structure
        """
        if not self.openai_client:
            return self._map_task_result_to_structure(task_result, required_structure)
        
        try:
            system_prompt = f"""You are enhancing analysis results to match a required JSON structure.

REQUIRED OUTPUT STRUCTURE:
{json.dumps(required_structure, indent=2)}

Take the existing task results and map/calculate values to match the required structure exactly.
- Use existing values where available
- Calculate missing values if possible from existing data
- Return "N/A" or appropriate defaults for missing data
- Maintain data types (numbers as numbers, strings as strings)

Return ONLY the JSON with the required structure."""

            user_message = f"""
ORIGINAL QUESTION: {question}

EXISTING TASK RESULTS:
{json.dumps(task_result, indent=2, default=str)}

Map these results to the required JSON structure."""

            response = await self.openai_client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=2000,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            enhanced_result = json.loads(response.choices[0].message.content)
            return enhanced_result
            
        except Exception as e:
            logger.error(f"LLM enhancement failed: {e}")
            return self._map_task_result_to_structure(task_result, required_structure)
    
    def _map_task_result_to_structure(self, task_result: Dict[str, Any], 
                                     required_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simple mapping of task results to required structure
        """
        mapped = {}
        
        for field, default_value in required_structure.items():
            if field in task_result:
                mapped[field] = task_result[field]
            else:
                # Try to find similar fields
                field_lower = field.lower()
                found = False
                
                for key, value in task_result.items():
                    key_lower = key.lower()
                    if (field_lower in key_lower or key_lower in field_lower):
                        mapped[field] = value
                        found = True
                        break
                
                if not found:
                    mapped[field] = default_value
        
        return mapped
    
    def _validate_and_fix_structure(self, result: Dict[str, Any], 
                                   required_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate the result and fix any missing or incorrect fields
        """
        validated = {}
        
        for field, default_value in required_structure.items():
            if field in result:
                value = result[field]
                
                if isinstance(default_value, str) and default_value.startswith("data:image"):
                    if isinstance(value, str) and value.startswith("data:image"):
                        validated[field] = value
                    else:
                        validated[field] = "data:image/png;base64,"
                elif isinstance(default_value, (int, float)):
                    try:
                        if isinstance(default_value, int):
                            validated[field] = int(float(value))
                        else:
                            validated[field] = float(value)
                    except (ValueError, TypeError):
                        validated[field] = default_value
                elif isinstance(default_value, str):
                    validated[field] = str(value)
                else:
                    validated[field] = value
            else:
                validated[field] = default_value
        
        return validated
    
    def _create_fallback_response(self, question: str) -> Dict[str, Any]:
        """
        Create a fallback response when analysis fails
        """
        structure = self._extract_json_structure(question)
        
        fallback = {}
        for field, default_value in structure.items():
            fallback[field] = default_value
        
        if not any("chart" in k or "plot" in k for k in fallback.keys()):
            fallback["error"] = "Analysis failed - returning default structure"
        
        return fallback
