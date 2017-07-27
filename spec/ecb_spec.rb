require "ecb"
require "tempfile"

module Ecb
  describe Parser do
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

    it "handles entries that do not have a valid date" do
      expect(
        described_class.read("spec/fixtures/usdeur-nodate.csv")
      ).to eq(
        [
          ExchangeRate.new(:usd, :eur, Date("2011-12-27"), 1.3069),
          ExchangeRate.new(:usd, :eur, Date("2011-12-23"), 1.3057),
        ]
      )
    end

    it "handles entries that do not have a valid rate" do
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

    it "retrieves stored value for a given key" do
      persistence.save([
        ExchangeRate.new(:usd, :eur, Date("2017-07-25"), 1.1694),
        ExchangeRate.new(:usd, :eur, Date("2017-07-24"), 1.1648),
      ])

      expect(
        persistence.retrieve(Date("2017-07-25"))
      ).to eq(1.1694)
    end

    it "falls back to nearest previous available date when given date does not have an exchange rate" do
      persistence.save([
        ExchangeRate.new(:usd, :eur, Date("2017-01-19"), 1.0668),
        ExchangeRate.new(:usd, :eur, Date("2017-01-11"), 1.0503),
      ])

      aggregate_failures do
        expect(persistence.retrieve(Date("2017-01-18"))).to eq(1.0503)
        expect(persistence.retrieve(Date("2017-01-17"))).to eq(1.0503)
        expect(persistence.retrieve(Date("2017-01-16"))).to eq(1.0503)
        expect(persistence.retrieve(Date("2017-01-15"))).to eq(1.0503)
        expect(persistence.retrieve(Date("2017-01-14"))).to eq(1.0503)
        expect(persistence.retrieve(Date("2017-01-13"))).to eq(1.0503)
        expect(persistence.retrieve(Date("2017-01-12"))).to eq(1.0503)
      end
    end

    it "raises ArgumentError for dates before 2000" do
      expect do
        persistence.retrieve(Date.civil(1999))
      end.to raise_error(ArgumentError, /before 2000/)
    end
  end

  describe Conversion do
    let(:exchange) { double("exchange") }
    let(:date) { Date("2011-03-05") }
    subject(:conversion) { described_class.new(exchange) }

    it "converts given amount of USD on given date to amount in EUR" do
      allow(exchange).to receive(:retrieve)
        .with(date)
        .and_return(BigDecimal("1.3957"))

      expect(
        conversion.usd_to_eur(120, date)
      ).to eq(BigDecimal("85.978362112201762556423300137"))
    end
  end
end
