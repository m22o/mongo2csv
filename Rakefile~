require "pry"
require "mongo"
require "csv"
require 'dotenv'
Dotenv.load


QUERY_NUMBER = 3
namespace :airbnb do
  desc "convert_csv"
  task "to_csv"  do
    db = Mongo::Client.new([ENV["HOST"]], :database => ENV["DB_NAME"])
    rooms = db[ENV["MONGO_COLLECTION"]].find().limit(QUERY_NUMBER).to_a
    clean_data = []
    binding.pry
    types = ["id","star_rating","amenities","country","property_type","reviews_count","room_type","star_rating"]
    rooms.each do |result|
      # if results[0]["payload"]["country"] == "Japan"
      clean_data << [
        result[types[0]],
        result["payload"][types[1]],
        result["payload"][types[2]].to_s,
        result["payload"][types[3]],
        result["payload"][types[4]],
        result["payload"][types[5]],
        result["payload"][types[6]],
        result["payload"][types[7]],
        result["payload"][types[8]],
      ]
      # end
    end
    CSV.open('airbnb.csv','w') do |data|
      data << types
      clean_data.each do |d|
        data << d
      end
    end
  end
end
