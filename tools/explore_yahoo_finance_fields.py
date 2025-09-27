import yfinance as yf
import pandas as pd

def explore_yfinance_fields(ticker_symbol: str):
    ticker = yf.Ticker(ticker_symbol)

    income_fields = ticker.income_stmt.index.tolist()
    balance_fields = ticker.balance_sheet.index.tolist()
    cashflow_fields = ticker.cashflow.index.tolist()
    info_keys = list(ticker.info.keys())

    output_lines = []

    def add_section(title, items):
        output_lines.append([title])
        output_lines.extend([[item] for item in items])
        output_lines.append([""])

    add_section("info_fields", info_keys)
    add_section("income_stmt_fields", income_fields)
    add_section("balance_sheet_fields", balance_fields)
    add_section("cashflow_fields", cashflow_fields)

    df = pd.DataFrame(output_lines, columns=["Field"])
    filename = f"{ticker_symbol}_yfinance_fields_list.csv"
    df.to_csv(filename, index=False)

    print(f"\n✅ Saved structured field list to {filename}")

if __name__ == "__main__":
    ticker_input = input("Enter ticker symbol (e.g., AAPL): ").strip().upper()
    explore_yfinance_fields(ticker_input)
