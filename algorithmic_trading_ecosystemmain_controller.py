"""
Main Orchestration Controller for Autonomous Trading Ecosystem
Coordinates all subsystems and manages the execution lifecycle
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import traceback

from .data_acquisition.market_data import MarketDataEngine
from .strategy_development.strategy_agent import StrategyDevelopmentAgent
from .risk_management.risk_manager import RiskManagementSystem
from .execution.trade_executor import TradeExecutionEngine
from .performance.analyzer import PerformanceAnalyzer
from .utils.firebase_client import FirebaseClient
from .utils.logging_config import get_logger

logger = get_logger(__name__)

class TradingEcosystemController:
    """Main controller orchestrating all trading ecosystem components"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the trading ecosystem controller
        
        Args:
            config: Configuration dictionary with system parameters
        """
        self.config = config or {}
        self.is_running = False
        self.start_time = None
        self.cycle_count = 0
        
        # Initialize components
        self._initialize_components()
        
    def _initialize_components(self) -> None:
        """Initialize all ecosystem components with proper error handling"""
        try:
            # Initialize Firebase for state management
            self.firebase_client = FirebaseClient(
                project_id=self.config.get('firebase_project_id'),
                credentials_path=self.config.get('firebase_credentials')
            )
            
            # Initialize core engines
            self.market_data = MarketDataEngine(
                exchanges=self.config.get('exchanges', ['binance', 'coinbase']),
                firebase_client=self.firebase_client
            )
            
            self.strategy_agent = StrategyDevelopmentAgent(
                firebase_client=self.firebase_client,
                model_path=self.config.get('model_path', 'models/strategies')
            )
            
            self.risk_manager = RiskManagementSystem(
                firebase_client=self.firebase_client,
                max_drawdown=self.config.get('max_drawdown', 0.15),
                max_position_size=self.config.get('max_position_size', 0.1)
            )
            
            self.trade_executor = TradeExecutionEngine(
                firebase_client=self.firebase_client,
                paper_trading=self.config.get('paper_trading', True)
            )
            
            self.performance_analyzer = PerformanceAnalyzer(
                firebase_client=self.firebase_client
            )
            
            logger.info("All ecosystem components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ecosystem components: {str(e)}")
            logger.error(traceback.format_exc())
            raise
            
    async def start(self) -> None:
        """Start the autonomous trading ecosystem"""
        if self.is_running:
            logger.warning("Ecosystem already running")
            return
            
        self.is_running = True
        self.start_time = datetime.utcnow()
        
        logger.info(f"Starting Autonomous Trading Ecosystem at {self.start_time}")
        
        try:
            # Main execution loop
            while self.is_running:
                await self._execution_cycle()
                await asyncio.sleep(self.config.get('cycle_interval', 60))  # Default 60 seconds
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            await self.stop()
        except Exception as e:
            logger.error(f"Critical error in main loop: {str(e)}")
            await self._emergency_shutdown()
            raise
            
    async def _execution_cycle(self) -> None:
        """Execute one complete trading cycle"""
        cycle_start = datetime.utcnow()
        self.cycle_count += 1
        
        logger.info(f"Starting trading cycle #{self.cycle_count}")
        
        try:
            # Phase 1: Data Acquisition
            market_data = await self.market_data.fetch_latest_data()
            
            # Phase 2: Strategy Development
            strategy_signal = await self.strategy_agent.develop_strategy(market_data)
            
            # Phase 3: Risk Assessment
            risk_approved = await self.risk_manager.assess_trade(
                strategy_signal, 
                market_data
            )
            
            if risk_approved:
                # Phase 4: Execution
                execution_result = await self.trade_executor.execute_trade(
                    strategy_signal,
                    market_data
                )
                
                # Phase 5: Performance Analysis
                await self.performance_analyzer.analyze_trade(
                    execution_result,
                    strategy_signal,
                    market_data
                )
            else:
                logger.info("Trade rejected by risk management system")
                
            # Phase 6: System Optimization
            await self._optimize_systems()
            
            cycle_duration = (datetime.utcnow() - cycle_start).total_seconds()
            logger.info(f"Cycle #{self.cycle_count} completed in {cycle_duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in execution cycle #{self.cycle_count}: {str(e)}")
            logger.error(traceback.format_exc())
            await self._handle_cycle_failure(e)
            
    async def _optimize_systems(self) -> None:
        """Optimize all systems based on recent performance"""
        try:
            # Update strategy agent with latest performance data
            performance_data = await self.performance_analyzer.get_recent_performance()
            await self.strategy_agent.optimize_strategies(performance_data)
            
            # Update risk parameters
            await self.risk_manager.update_risk_parameters(performance_data)
            
            logger.debug("System optimization completed")
            
        except Exception as e:
            logger.error(f"Failed to optimize systems: {str(e)}")
            
    async def _handle_cycle_failure(self, error: Exception) -> None:
        """Handle failures in execution cycle"""