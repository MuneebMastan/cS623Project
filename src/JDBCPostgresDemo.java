import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCPostgresDemo {
	private Connection conn;
	private Statement stmt = null;

	public JDBCPostgresDemo() throws SQLException {
		this.setConnection();
		
	}

	public void setConnection() throws SQLException {

		conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");

		// For atomicity
		conn.setAutoCommit(false);

		// For isolation
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

	}

	public void createTable() throws SQLException {

		try {
			stmt = conn.createStatement();
			String createTableProd = "CREATE TABLE Product(prod_id CHAR(10), pname VARCHAR(30), price decimal)";
			String createTableDepot = "create Table Depot(Dep_id char(20),address varchar(25),volume integer)";
			String createTableStock = "create table Stock(prod_id char(20),Dep_id char(20),quantity integer)";
			stmt.executeUpdate(createTableProd);
			stmt.executeUpdate(createTableDepot);
			stmt.executeUpdate(createTableStock);

		} catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();
			// For atomicity
			conn.rollback();
			stmt.close();
			conn.close();
			return;
		}
		conn.commit();
		System.out.println("Table Created ");
		stmt.close();

	}

	public void addContraints() throws SQLException {

		try {
			stmt = conn.createStatement();
			String alterTableProd = "alter table Product add constraint pk_product  primary key(prod_id)";
			String alterTableDepot = "alter table depot add constraint pk_depot primary key(dep_id)";
			String alterTableStock = "alter table stock add constraint pk_stock primary key(prod_id,dep_id)";
			String fkProdStock = "alter table stock add constraint fk_stock_product foreign key(prod_id) references product(prod_id) ON DELETE CASCADE";
			String fkDepotStock = "alter table stock add constraint fk_stock_depot foreign key(dep_id) references depot(dep_id) ON DELETE CASCADE";
			stmt.execute(alterTableProd);
			stmt.execute(alterTableDepot);
			stmt.execute(alterTableStock);
			stmt.execute(fkProdStock);
			stmt.execute(fkDepotStock);

		} catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();
			// For atomicity
			conn.rollback();
			stmt.close();
			conn.close();
			return;
		}
		conn.commit();
		System.out.println("Table Altered ");
		stmt.close();
		

	}

	public void insertValues() throws SQLException {

		
		try {
			stmt = conn.createStatement();
			stmt.execute("insert into product values('P1', 'tape', '2.5')");
			stmt.execute("insert into product values('P2', 'TV', '250')");
			stmt.execute("insert into product values('P3', 'VCR', '80')");

			stmt.execute("insert into depot values('D1', 'New York', 9000)");
			stmt.execute("insert into depot values('D2', 'Syracuse',6000)");
			stmt.execute("insert into depot values('D3', 'New York',2000)");

			stmt.execute("insert into stock values('P1', 'D2',1000)");
			stmt.execute("insert into stock values('P1', 'D3',1200)");
			stmt.execute("insert into stock values('P3', 'D1',3000)");
			stmt.execute("insert into stock values('P3', 'D3',2000)");
			stmt.execute("insert into stock values('P2', 'D3',1500)");
			stmt.execute("insert into stock values('P2', 'D1',-400)");
			stmt.execute("insert into stock values('P2', 'D2',2000)");

		}  catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();
			// For atomicity
			conn.rollback();
			stmt.close();
			conn.close();
			return;
		}
		conn.commit();
		System.out.println("Values Inserted ");
		stmt.close();

	}
	
	public void doTransactions() throws SQLException {
		try {
			stmt = conn.createStatement();
			stmt.execute("delete from depot where dep_id='D1'");
		}
		 catch (SQLException e) {
				System.out.println("An exception was thrown");
				e.printStackTrace();
				// For atomicity
				conn.rollback();
				stmt.close();
				conn.close();
				return;
			}
			conn.commit();
			System.out.println("Value Deleted ");
			stmt.close();
	}

	public static void main(String args[]) throws SQLException {
		JDBCPostgresDemo postgres = new JDBCPostgresDemo();
		postgres.setConnection();
		postgres.createTable();
		postgres.addContraints();
		postgres.insertValues();
		postgres.doTransactions();
		postgres.conn.close();

	}

}
