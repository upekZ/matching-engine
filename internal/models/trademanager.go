package models

type TradeManager struct {
	volume int
	trades []*Trade
}

func NewTradeManager() *TradeManager {
	return &TradeManager{
		volume: 0,
		trades: make([]*Trade, 0),
	}
}

func (t *TradeManager) AddTrade(trade *Trade) {
	t.volume += trade.Quantity
	t.trades = append(t.trades, trade)
}

func (t *TradeManager) Append(trades *TradeManager) {
	t.volume += trades.volume
	t.trades = append(t.trades, trades.trades...)
}

func (t *TradeManager) GetVolume() int {
	return t.volume
}

func (t *TradeManager) GetTrades() []*Trade {
	return t.trades
}
