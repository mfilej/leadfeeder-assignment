require "ecb"
require "tempfile"

module Ecb
  describe Parse do
    it "parses exchange rates" do
      expect(
        described_class.read("spec/fixtures/usdeur-short.csv")
      ).to eq(
        [
          ExchangeRate.new(:usd, :eur, Date("2017-07-26"), 1.1644),
          ExchangeRate.new(:usd, :eur, Date("2017-07-25"), 1.1694),
          ExchangeRate.new(:usd, :eur, Date("2017-07-24"), 1.1648),
        ]
      )
    end

    it "handles entries that do not have a rate" do
      expect(
        described_class.read("spec/fixtures/usdeur-norate.csv")
      ).to eq(
        [
          ExchangeRate.new(:usd, :eur, Date("2011-12-27"), 1.3069),
          ExchangeRate.new(:usd, :eur, Date("2011-12-23"), 1.3057),
        ]
      )
    end
  end

  describe Persistence do
    let(:db_file) { Tempfile.new }
    subject(:persistence) { described_class.new(path: db_file) }

    it "saves an array of exchange rates to PStore" do
      persistence.save([
        ExchangeRate.new(:usd, :eur, Date("2017-07-25"), 1.1694),
        ExchangeRate.new(:usd, :eur, Date("2017-07-24"), 1.1648),
      ])

      PStore.new(db_file).tap do |pstore|
        pstore.transaction(true) do
          aggregate_failures do
            expect(pstore[Date("2017-07-25")]).to eq(1.1694)
            expect(pstore[Date("2017-07-24")]).to eq(1.1648)
          end
        end
      end
    end
  end
end
