require "spec_helper"

describe SqlExercise do

  let(:sql_exercise) { SqlExercise.new }

  describe "#all_customers" do
    it "returns a hash of all the customer data" do
      customers = sql_exercise.all_customers

      expect(customers.length).to eq(10)

      first_customer = customers.first

      expect(first_customer["name"]).to eq("Donato Rempel")
      expect(first_customer["email"]).to eq("ladarius@waelchi.org")
    end
  end

  describe "#limit_customers" do
    it "can return one customer" do
      expected_customers = [
        {
          "id" => "1",
          "name" => "Donato Rempel",
          "email" => "ladarius@waelchi.org",
          "address" => "890 Ullrich Plains",
          "city" => "Janachester",
          "state" => "Virginia",
          "zipcode" => "77714"
        }
      ]

      expect(sql_exercise.limit_customers(1)).to eq(expected_customers)
    end

    it "can return three customers" do

      expected_customers = [
        {
          "id" => "1",
          "name" => "Donato Rempel",
          "email" => "ladarius@waelchi.org",
          "address" => "890 Ullrich Plains",
          "city" => "Janachester",
          "state" => "Virginia",
          "zipcode" => "77714"
        },
        {
          "id" => "2",
          "name" => "Tyrell Von DDS",
          "email" => "cleo_frami@bartondenesik.name",
          "address" => "63337 Abdullah Camp",
          "city" => "Verdieborough",
          "state" => "Colorado",
          "zipcode" => "69882-7027"
        },
        {
          "id" => "3",
          "name" => "Ms. Sofia Rowe",
          "email" => "jacky_funk@bayerprosacco.name",
          "address" => "1991 Kyler Village",
          "city" => "Opheliaborough",
          "state" => "Ohio",
          "zipcode" => "15599-5395"
        }
      ]

      expect(sql_exercise.limit_customers(3)).to eq(expected_customers)
    end
  end

  describe "#order_customers" do
    it "can orders the customers in alphabetical order" do
      alphabetical_customers = sql_exercise.order_customers("ASC")

      expect(alphabetical_customers.first["name"]).to eq("Coleman Prohaska Jr.")
      expect(alphabetical_customers.last["name"]).to eq("Zelma Davis")
    end

    it "can orders the customers in reverse alphabetical order" do
      alphabetical_customers = sql_exercise.order_customers("DESC")

      expect(alphabetical_customers.first["name"]).to eq("Zelma Davis")
      expect(alphabetical_customers.last["name"]).to eq("Coleman Prohaska Jr.")
    end
  end

  describe "#id_and_name_for_customers" do
    it "returns only the id and name for each customer" do
      customers = sql_exercise.id_and_name_for_customers

      expect(customers.length).to eq(10)

      first_customer = customers.first

      expect(first_customer).to eq({"id" => "1", "name" => "Donato Rempel"})
    end
  end

  describe "#all_items" do
    it "returns all items" do
      items = sql_exercise.all_items

      expect(items.length).to eq(9)

      first_item = items.first

      expect(first_item["name"]).to eq("ski01")
      expect(first_item["description"]).to eq("downhill ski")
    end
  end

  describe "#find_item_by_name" do
    it "returns the item with the given name" do
      expect(sql_exercise.find_item_by_name("boot01")).to eq({"id" => "7", "name" => "boot01", "description" => "hiking boots"})
    end

    it "returns nil if no item is found" do
      expect(sql_exercise.find_item_by_name("nonehere")).to eq(nil)
    end
  end

  describe "#count_customers" do
    it "returns the number of customers" do
      expect(sql_exercise.count_customers).to eq(10)
    end
  end

  describe "#sum_order_amounts" do
    it "returns the total of all order amounts" do
      sum = sql_exercise.sum_order_amounts

      expect(sum).to eq(2466.41)
    end
  end

  describe "#minimum_order_amount_for_customers" do
    it "returns the minimum order amount for each customer" do
      minimum_order_amounts = sql_exercise.minimum_order_amount_for_customers

      customer_1 = minimum_order_amounts.find { |order| order["customer_id"] == "1" }

      expect(customer_1).to eq({"customer_id" => "1", "min" => "5.67"})
    end
  end

  describe "#customer_order_totals" do
    it "returns the customer id, customer name, and total of all their orders for each customer" do
      orders = sql_exercise.customer_order_totals

      order_10 = orders.find { |order| order["customer_id"] == "10" }

      expect(order_10).to eq({"customer_id" => "10", "name" => "Hulda Will III", "sum" => "313.55"})
    end
  end

  describe "#items_ordered_by_user" do
    it "returns the item name for all the orders placed by the given customer id" do
      expected_orders_for_second_customer = [
        "boot01",
        "bike03",
        "ski01",
        "bike01",
        "bike02",
        "boot03",
        "ski02",
        "boot01"
      ]

      expect(sql_exercise.items_ordered_by_user(2)).to eq(expected_orders_for_second_customer)
    end
  end

  describe "#customers_that_bought_item" do
    it "returns the correct customer names and ids for customers that bought 'bike01'" do
      expected_customers = [
        {
          "customer_name" => "Tyrell Von DDS", "id" => "2"
        },
        {
          "customer_name" => "Edna Hintz", "id" => "7"
        },
        {
          "customer_name" => "Evert Pfeffer", "id" => "4"
        },
        {
          "customer_name" => "Donato Rempel", "id" => "1"
        },
        {
          "customer_name" => "Coleman Prohaska Jr.", "id" => "8"
        },
        {
          "customer_name" => "Zelma Davis", "id" => "6"
        },
        {
          "customer_name" => "Elta Dicki", "id" => "5"
        },
        {
          "customer_name" => "Hulda Will III", "id" => "10"
        },
        {
          "customer_name" => "Wilhelmine Huels", "id" => "9"
        },
      ]

      expect(sql_exercise.customers_that_bought_item("bike01")).to match_array(expected_customers)
    end

    it "returns the correct customer names and ids for customers that bought 'boot01'" do
      expected_customers = [
        {
          "customer_name" => "Tyrell Von DDS", "id" => "2"
        },
        {
          "customer_name" => "Edna Hintz", "id" => "7"
        },
        {
          "customer_name" => "Ms. Sofia Rowe", "id" => "3"
        },
        {
          "customer_name" => "Donato Rempel", "id" => "1"
        },
        {
          "customer_name" => "Coleman Prohaska Jr.", "id" => "8"
        },
        {
          "customer_name" => "Hulda Will III", "id" => "10"
        },
        {
          "customer_name" => "Zelma Davis", "id" => "6"
        },
        {
          "customer_name" => "Elta Dicki", "id" => "5"
        },
        {
          "customer_name" => "Wilhelmine Huels", "id" => "9"
        },
      ]

      expect(sql_exercise.customers_that_bought_item("boot01")).to match_array(expected_customers)
    end
  end

  describe "#customers_that_bought_item_in_state" do
    it "returns the first customer that bought 'bike03' that lives in Maine" do
      expected_user = {
        "id" => "9",
        "name" => "Wilhelmine Huels",
        "email" => "alexander_rice@ornjakubowski.com",
        "address" => "44097 Elvie Divide",
        "city" => "West Lauryntown",
        "state" => "Maine",
        "zipcode" => "55164-9178"
      }

      expect(sql_exercise.customers_that_bought_item_in_state("bike03", "Maine")).to eq(expected_user)
    end
  end

  it "does not allow users of the SqlExercise class mess with the database" do
    skip
    sql_exercise.limit_customers("5; DROP TABLE customers;")

    expect {
      expect(sql_exercise.database_connection.sql("SELECT count(*) FROM customers;"))
    }.to_not raise_exception

    sql_exercise.order_customers("ASC; DROP TABLE customers;")

    expect {
      expect(sql_exercise.database_connection.sql("SELECT count(*) FROM customers;"))
    }.to_not raise_exception

    sql_exercise.order_customers("ASC; DROP TABLE customers;")

    expect {
      expect(sql_exercise.database_connection.sql("SELECT count(*) FROM customers;"))
    }.to_not raise_exception

    sql_exercise.find_items_by_name("'; drop table items; SELECT * FROM customers where name = '")

    expect {
      expect(sql_exercise.database_connection.sql("SELECT count(*) FROM items;"))
    }.to_not raise_exception
  end
end
