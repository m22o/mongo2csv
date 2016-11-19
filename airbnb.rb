require "pry"
require "mongo"
require 'json'
require "csv"

QUERY_NUMBER = 3
namespace :airbnb do
  desc "convert_csv"
  task "to_csv" => :environment do
    db = Mongo::Client.new([ '163.143.138.132'], :database => 'airbnb')
    rooms_json  = db["rooms"].find().limit(QUERY_NUMBER).to_json
    results = JSON.parse(rooms_json)
    clean_data = []
    types = ["id","star_rating","amenities","country","property_type","reviews_count","room_type","star_rating"]
    results.each do |result|
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
