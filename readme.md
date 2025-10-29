## This project is WIP hence not fully developed.

# TOAD Trade 2.0
Welcome to my new portfolio generation and management program. This aims to be far more comprehensive than TOAD Trade 1.0 incorporating a deeper, higher level analysis of markets and making bets on it. 

Another way TT2 differs dramatically from TT1 is its portfolio generation algorithms. TT1 focuses on implementing technical indicators on a given stock and combines them in some brutal, rudimentary way. TT2 will dynamically generate the portfolio and eloquently analyse its history to predict buy and sell signals.

Importantly, I will personally use this algorithm with real money and track that portfolios performance, as a proof of concept. 

I hope to use my extensive knowledge in Mathematics, strong programming skills and emerging financial knowledge to produce something quite fantastic. 

## What Data Does TT2 Use?
TT2 needs to have a dynamic system that fetches data from various different sources. A key part of my strategy is to obtain such valuable data through automated processes and to work with this data again in an automated process. The data points I am including are,
1. Broad market indexes like the S&P500,
2. New listings, IPOs,
3. Alternative listings like SPACs
4. Most traded stocks,
5. Top gainers & losers,
6. New 52 week highs/ lows,
7. Upcoming & recent earning dates, 
8. Analyst rating changes,
9. Insider transactions,
10. Short interest,
11. Volatility index,
12. Google trends data,
13. Historical volatility,
14. Liquidity & tradability,
15. Interest rates,
16. Inflation rates,
17. Advance/ decline.

## TOADTrade2 Development Roadmap

### Phase 1: Foundation & Infrastructure
- [x] **Data Pipeline:** Set up a robust data pipeline using a proper database (e.g., PostgreSQL/TimescaleDB) for storing and retrieving clean historical data.
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
