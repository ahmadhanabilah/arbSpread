# 🤖 ArbSpread Bot — Logic & Config Overview (User FAQ)

This guide explains how your bot trades between **Lighter** and **Extended**,
and what each configuration setting actually means — in plain English.

---

## 🧠 What the Bot Does

The bot automatically monitors the **price spread** between two exchanges:

* **Lighter** (zk-rollup perpetual DEX)
* **Extended** (Starknet perpetual DEX)

Whenever one is cheaper than the other by a safe margin,
it buys on the cheaper side and sells on the expensive side — locking in small, steady profits.

It repeats this all day, using your custom settings from the configuration file.

---
## ⚙️ Your Config Parameters Explained

| Parameter| Description | Example 
| -------- | ----------- | ------  
| `TRADES INTERVAL`             | Sleep Time after placing a Trade in second | `1` |
| `MIN SPREAD ENTRY`            | Minimum spread (%) required to open a trade. Example: EntryLE if spreadLE > 0.2% viceversa| `0.20` |
| `SPREAD MULTIPLIER`           | Look at **DCA & Averaging** Part Below | `0.20` |
| `MIN SPREAD EXIT DIFF`        | Min Spread difference to Exit the Trades, with 0.15% you will basically capture 0.15% Profit before Fees | `0.15`
| `MIN TRADE VALUE`             | Minimum Notional for a trade (Entry & Exit) | `50` 
| `MAX TRADE VALUE ENTRY`       | Minimum Notional for an Entry Trade |  `500` 
| `MAX TRADE VALUE EXIT`        | Minimum Notional for an Exit Trade |  `500` 
| `PERC OF OB`                  | Example : 10%, then we will only take maximum 10% of available orderbook  | `10%`
| `MAX INVENTORY VALUE`         | Maximum Notional you Hold (single symbol) | `5000`
| `INV LEVEL TO MULT SPREAD`    | Look at **DCA & Averaging** Part Below | `5`

---

## ⚡ When It Trades

### 🟢 Entry-LE

* Condition : Lighter cheaper, Extended more expensive
* Action    : **Buy** on Lighter, **Sell** on Extended

### 🔴 Exit-EL

* Condition: we have Enough Difference to Exit (following your configs)
* Action: **Sell** on Lighter, **Buy** on Extended (Reducing Position)

---

### 🟢 Entry-EL

* Condition : Extended cheaper, Lighter more expensive
* Action: **Buy** on Extended, **Sell** on Lighter

### 🔴 Exit-LE

* Condition: we have Enough Difference to Exit (following your configs)
* Action: **Sell** on Extended, **Buy** on Lighter (Reducing Position)

---

## 🪜 DCA & Averaging

If spreads keep widening, the bot will increase the MIN SPREAD TO ENTRY

if not, the bot can't exit fast, so we dont generate more volumes and more profits

then we can have a better average Spread for our entry, so we can exit faster (more volume, more profit)

this one needs a Config Tuning periodically

| EXAMPLE |  |
| ----- | ------ |
| MIN SPREAD | 0.5 |
| SPREAD MULTIPLIER | 1.5 |
| MAX INVENTORY | 5000 |
| INV LEVEL TO MULT SPREAD | 5 |

| INV VALUE | INV LEVEL | Calculation   | Min Spread To Entry   
| -----     | ------    | -----         | -----                 
| 0         | 1         | `0.5*1.5*1`   | 0.5     
| 1000      | 2         | `0.5*1.5*2`   | 1.5     
| 2000      | 3         | `0.5*1.5*3`   | 2.25    
| 3000      | 4         | `0.5*1.5*4`   | 3
| 4000      | 5         | `0.5*1.5*5`   | 3.75


---

## ⚖️ Telegram Notification

1. Trades
2. Criticals

