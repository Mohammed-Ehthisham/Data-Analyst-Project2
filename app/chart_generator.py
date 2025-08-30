"""
Chart Generation Module for LLM-Driven Data Analyst Agent
Provides reliable, programmatic chart generation to supplement LLM outputs
"""

import base64
import io
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List, Tuple, Optional, Any


class ChartGenerator:
    """Generates matplotlib charts with consistent base64 encoding under 100KB"""
    
    def __init__(self):
        # Set matplotlib backend to Agg for server environments
        plt.switch_backend('Agg')
        # Set default style for professional charts
        plt.style.use('default')
    
    def create_bar_chart(self, data: List[Tuple[str, float]], 
                        title: str = "Bar Chart", 
                        xlabel: str = "Category", 
                        ylabel: str = "Value",
                        color: str = "steelblue") -> str:
        """
        Create a bar chart from data
        
        Args:
            data: List of (label, value) tuples
            title: Chart title
            xlabel: X-axis label
            ylabel: Y-axis label
            color: Bar color
            
        Returns:
            Base64 encoded PNG image
        """
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if not data:
                # Create empty chart with message
                ax.text(0.5, 0.5, 'No data available', 
                       horizontalalignment='center', verticalalignment='center',
                       transform=ax.transAxes, fontsize=14)
                ax.set_title(title)
                return self._save_to_base64(fig)
            
            labels, values = zip(*data)
            
            # Create bar chart
            bars = ax.bar(labels, values, color=color, alpha=0.7)
            
            # Customize chart
            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            ax.set_xlabel(xlabel, fontsize=12)
            ax.set_ylabel(ylabel, fontsize=12)
            
            # Rotate x-axis labels if they're long
            if any(len(str(label)) > 8 for label in labels):
                plt.xticks(rotation=45, ha='right')
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}', ha='center', va='bottom')
            
            # Adjust layout
            plt.tight_layout()
            
            return self._save_to_base64(fig)
            
        except Exception as e:
            return self._create_error_chart(f"Bar chart error: {str(e)}")
        finally:
            plt.close(fig)
    
    def create_line_chart(self, data: List[Tuple[Any, float]], 
                         title: str = "Line Chart",
                         xlabel: str = "X-axis", 
                         ylabel: str = "Y-axis",
                         color: str = "darkblue") -> str:
        """
        Create a line chart from data
        
        Args:
            data: List of (x_value, y_value) tuples
            title: Chart title
            xlabel: X-axis label
            ylabel: Y-axis label
            color: Line color
            
        Returns:
            Base64 encoded PNG image
        """
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if not data:
                ax.text(0.5, 0.5, 'No data available', 
                       horizontalalignment='center', verticalalignment='center',
                       transform=ax.transAxes, fontsize=14)
                ax.set_title(title)
                return self._save_to_base64(fig)
            
            x_values, y_values = zip(*data)
            
            # Create line chart
            ax.plot(x_values, y_values, color=color, linewidth=2, marker='o', markersize=4)
            
            # Customize chart
            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            ax.set_xlabel(xlabel, fontsize=12)
            ax.set_ylabel(ylabel, fontsize=12)
            
            # Add grid
            ax.grid(True, alpha=0.3)
            
            # Rotate x-axis labels if needed
            if len(x_values) > 10 or any(len(str(x)) > 8 for x in x_values):
                plt.xticks(rotation=45, ha='right')
            
            plt.tight_layout()
            
            return self._save_to_base64(fig)
            
        except Exception as e:
            return self._create_error_chart(f"Line chart error: {str(e)}")
        finally:
            plt.close(fig)
    
    def create_scatter_plot(self, data: List[Tuple[float, float]], 
                           title: str = "Scatter Plot",
                           xlabel: str = "X-axis", 
                           ylabel: str = "Y-axis",
                           color: str = "red",
                           add_regression: bool = False) -> str:
        """
        Create a scatter plot from data
        
        Args:
            data: List of (x_value, y_value) tuples
            title: Chart title
            xlabel: X-axis label
            ylabel: Y-axis label
            color: Point color
            add_regression: Whether to add regression line
            
        Returns:
            Base64 encoded PNG image
        """
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if not data:
                ax.text(0.5, 0.5, 'No data available', 
                       horizontalalignment='center', verticalalignment='center',
                       transform=ax.transAxes, fontsize=14)
                ax.set_title(title)
                return self._save_to_base64(fig)
            
            x_values, y_values = zip(*data)
            
            # Create scatter plot
            ax.scatter(x_values, y_values, color=color, alpha=0.6, s=30)
            
            # Add regression line if requested
            if add_regression and len(data) > 1:
                try:
                    z = np.polyfit(x_values, y_values, 1)
                    p = np.poly1d(z)
                    ax.plot(x_values, p(x_values), "r--", alpha=0.8, linewidth=2)
                except:
                    pass  # Skip regression if it fails
            
            # Customize chart
            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            ax.set_xlabel(xlabel, fontsize=12)
            ax.set_ylabel(ylabel, fontsize=12)
            
            # Add grid
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            return self._save_to_base64(fig)
            
        except Exception as e:
            return self._create_error_chart(f"Scatter plot error: {str(e)}")
        finally:
            plt.close(fig)
    
    def create_from_dataframe(self, df: pd.DataFrame, 
                             chart_type: str = "bar",
                             x_col: Optional[str] = None,
                             y_col: Optional[str] = None,
                             title: str = "Data Visualization") -> str:
        """
        Create chart directly from pandas DataFrame
        
        Args:
            df: pandas DataFrame
            chart_type: Type of chart ('bar', 'line', 'scatter')
            x_col: Column name for x-axis (if None, uses index)
            y_col: Column name for y-axis (if None, uses first numeric column)
            title: Chart title
            
        Returns:
            Base64 encoded PNG image
        """
        try:
            if df.empty:
                return self._create_error_chart("DataFrame is empty")
            
            # Auto-select columns if not specified
            if y_col is None:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) == 0:
                    return self._create_error_chart("No numeric columns found")
                y_col = numeric_cols[0]
            
            if x_col is None:
                # Use index or first non-numeric column
                non_numeric = df.select_dtypes(exclude=[np.number]).columns
                if len(non_numeric) > 0:
                    x_col = non_numeric[0]
                else:
                    x_data = df.index.tolist()
                    y_data = df[y_col].tolist()
            else:
                x_data = df[x_col].tolist()
                y_data = df[y_col].tolist()
            
            # Create data tuples
            if x_col is None:
                data = list(zip(x_data, y_data))
            else:
                data = list(zip(df[x_col].tolist(), df[y_col].tolist()))
            
            # Generate appropriate chart
            if chart_type.lower() == "line":
                return self.create_line_chart(data, title=title, 
                                            xlabel=x_col or "Index", ylabel=y_col)
            elif chart_type.lower() == "scatter":
                return self.create_scatter_plot(data, title=title,
                                              xlabel=x_col or "Index", ylabel=y_col)
            else:  # Default to bar
                return self.create_bar_chart(data, title=title,
                                           xlabel=x_col or "Index", ylabel=y_col)
                
        except Exception as e:
            return self._create_error_chart(f"DataFrame chart error: {str(e)}")
    
    def _save_to_base64(self, fig) -> str:
        """Convert matplotlib figure to base64 string under 100KB"""
        try:
            # Save to bytes buffer
            buffer = io.BytesIO()
            fig.savefig(buffer, format='png', dpi=80, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            buffer.seek(0)
            
            # Get image bytes
            image_bytes = buffer.getvalue()
            buffer.close()
            
            # Check size and reduce quality if needed
            if len(image_bytes) > 100_000:  # 100KB limit
                buffer = io.BytesIO()
                fig.savefig(buffer, format='png', dpi=60, bbox_inches='tight',
                           facecolor='white', edgecolor='none')
                buffer.seek(0)
                image_bytes = buffer.getvalue()
                buffer.close()
            
            # Encode to base64
            b64_string = base64.b64encode(image_bytes).decode('utf-8')
            return f"data:image/png;base64,{b64_string}"
            
        except Exception as e:
            return self._create_error_chart(f"Encoding error: {str(e)}")
    
    def _create_error_chart(self, error_msg: str) -> str:
        """Create a simple error message chart"""
        try:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.text(0.5, 0.5, f'Chart Error:\n{error_msg}', 
                   horizontalalignment='center', verticalalignment='center',
                   transform=ax.transAxes, fontsize=12, 
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="lightcoral", alpha=0.7))
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis('off')
            plt.tight_layout()
            
            result = self._save_to_base64(fig)
            plt.close(fig)
            return result
            
        except:
            # Ultimate fallback - return empty data URI
            return "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
