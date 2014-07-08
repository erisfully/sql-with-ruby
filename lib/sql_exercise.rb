require "database_connection"

class SqlExercise

  attr_reader :database_connection

  def initialize
    @database_connection = DatabaseConnection.new
  end

  def all_customers
    database_connection.sql("SELECT * from customers")
  end

  def limit_customers(num)
    database_connection.sql("SELECT * FROM customers LIMIT #{num}")
  end

  def order_customers(order)
    database_connection.sql("SELECT * FROM customers ORDER BY name #{order}")
  end

  def id_and_name_for_customers
    database_connection.sql("SELECT id, name FROM customers")
  end

  def all_items
    database_connection.sql("SELECT * FROM items")
  end

  def find_item_by_name(name)
    database_connection.sql("SELECT * FROM items WHERE name = '#{name}'").first
  end

  def count_customers
    hash = database_connection.sql("SELECT COUNT (*) FROM customers").first
    hash['count'].to_i
  end

  def sum_order_amounts
    hash = database_connection.sql("SELECT SUM(amount) FROM orders").first
    hash['sum'].to_f
  end

  def minimum_order_amount_for_customers
    database_connection.sql("SELECT customer_id, MIN(amount) FROM orders GROUP BY customer_id").find
  end

  def customer_order_totals
    database_connection.sql("SELECT orders.customer_id, customers.name, SUM(orders.amount) FROM orders INNER JOIN customers ON orders.customer_id = customers.id GROUP BY customer_id, customers.name")
  end

  def items_ordered_by_user(num)
    array_of_names = []
    array_of_hashes = database_connection.sql <<-SQL
    SELECT items.name
    FROM orderitems
    LEFT JOIN items
    ON orderitems.item_id = items.id
    INNER JOIN orders
    ON orderitems.order_id = orders.id
    WHERE customer_id = #{num}
    SQL
    array_of_hashes.each do |item|
      array_of_names.push(item["name"])
    end
    array_of_names
  end

  def customers_that_bought_item(item)
    database_connection.sql <<-SQL
    SELECT customers.name AS customer_name, customers.id
    FROM orderitems
    LEFT JOIN orders ON orderitems.order_id = orders.id
    LEFT JOIN customers ON orders.customer_id = customers.id
    INNER JOIN items ON orderitems.item_id = items.id
    WHERE items.name = '#{item}'
    GROUP BY customers.name, customers.id
    SQL
  end

  def customers_that_bought_item_in_state(item, state)
    command = <<-SQL
    SELECT customers.id, customers.name, customers.email, customers.address, customers.city, customers.state, customers.zipcode
    FROM orderitems
    LEFT JOIN orders ON orderitems.order_id = orders.id
    LEFT JOIN customers ON orders.customer_id = customers.id
    INNER JOIN items ON orderitems.item_id = items.id
    WHERE items.name = '#{item}' AND state = '#{state}'
    GROUP BY customers.name, customers.id
    SQL
    database_connection.sql(command).first

  end


end
