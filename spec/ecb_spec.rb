require "ecb"

module Ecb
  describe Parse do
    it "parses exchange rates" do
      expect(
        described_class.read("spec/fixtures/usdeur-short.csv")
      ).to eq(
        [
          ExchangeRate.new(:usd, :eur, date("2017-07-26"), 1.1644),
          ExchangeRate.new(:usd, :eur, date("2017-07-25"), 1.1694),
          ExchangeRate.new(:usd, :eur, date("2017-07-24"), 1.1648),
        ]
      )
    end

    it "handles entries that do not have a rate" do
      expect(
        described_class.read("spec/fixtures/usdeur-norate.csv")
      ).to eq(
        [
          ExchangeRate.new(:usd, :eur, date("2011-12-27"), 1.3069),
          ExchangeRate.new(:usd, :eur, date("2011-12-23"), 1.3057),
        ]
      )
    end

    def date(string)
      Date.parse(string)
    end
  end
end
