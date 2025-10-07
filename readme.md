## This project is WIP hence not fully developed.

# TOAD Trade 2.0
Welcome to my new portfolio generation and management program. This aims to be far more comprehensive than TOAD Trade 1.0 incorporating a deeper, higher level analysis of markets and making bets on it. 

Another way TT2 differs dramatically from TT1 is its portfolio generation algorithms. TT1 focuses on implementing technical indicators on a given stock and combines them in some brutal, rudimentary way. TT2 will dynamically generate the portfolio and eloquently analyse its history to predict buy and sell signals.

Importantly, I will personally use this algorithm with real money and track that portfolios performance, as a proof of concept. 

I hope to use my extensive knowledge in Mathematics, strong programming skills and emerging financial knowledge to produce something quite fantastic. 

## TOADTrade2 Development Roadmap

### Phase 1: Foundation & Infrastructure
- [ ] **Data Pipeline:** Set up a robust data pipeline using a proper database (e.g., PostgreSQL/TimescaleDB) for storing and retrieving clean historical data.
- [ ] **Backtesting Engine:** Upgrade the backtester to be event-driven, accounting for transaction costs, slippage, and realistic order fills.
- [ ] **Research Environment:** Containerize the entire research and backtesting stack using Docker for full reproducibility.

### Phase 2: Alpha Generation & Signal Research
- [ ] **Isolate Alphas:** Refactor existing strategies (momentum, mean reversion) into standalone, pure "alpha factors."
- [ ] **Quantify Performance:** Rigorously test each alpha individually using tools like `alphalens` to measure its Information Coefficient (IC), turnover, and decay.
- [ ] **Sophisticated Alphas:** Research and implement a more advanced alpha strategy, such as statistical arbitrage (pairs trading) or a market regime detection model.

### Phase 3: Portfolio Construction
- [ ] **Mean-Variance Optimization (MVO):** Implement an MVO model where alpha signal strength is used as a proxy for expected returns.
- [ ] **Advanced Portfolio Construction:** Implement a more advanced model like the Black-Litterman model or Hierarchical Risk Parity (HRP) to improve robustness.

### Phase 4: Risk Management & Deployment
- [ ] **Risk Framework:** Build a portfolio-level risk management system (e.g., volatility targeting, max drawdown limits, VaR/CVaR calculations).
- [ ] **Paper Trading:** Connect the complete system to a paper trading API (e.g., Alpaca) to test performance in a live environment.