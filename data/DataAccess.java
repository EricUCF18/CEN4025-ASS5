package week04.data;

/**
 * @author Eric Willoughby
 * @since 9/18/17
 * */

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.*;

import week04.util.AtmLogger;
import week04.app.User;

public class DataAccess {

	private final static Logger logger = Logger.getLogger(AtmLogger.ATM_LOGGER + "." + DataAccess.class.getName());
	private SimpleDateFormat m_formatter;

	private String m_password = DEFAULT_PASS;
	private String m_userName = DEFAULT_USER;
	private Connection m_connect = null;

	private PreparedStatement m_selectAllUsersStatement;
	private PreparedStatement m_selectUserByIdStatement;
	private PreparedStatement m_updateUserByIdStatement;
	private PreparedStatement m_deleteUserByIdStatement;
	private PreparedStatement m_insertUserStatement;
	private PreparedStatement m_lastInsertStatement;

	private static String INSERT_USER_SQL = "INSERT INTO atm.user(pin, first_name, last_name, last_update) values(?, ?, ?, ?)";
	private static String SELECT_USER_SQL = "SELECT id, pin, first_name, last_name FROM atm.user";
	private static String SELECT_USER_BY_ID_SQL = "SELECT id, pin, first_name, last_name FROM atm.user WHERE id=?";
	private static String DELETE_USER_BY_ID_SQL = "DELETE FROM atm.user WHERE id=?";
	private static String UPDATE_USER_BY_ID_SQL = "UPDATE atm.user SET pin=?, first_name=?, last_name=?, last_update=? WHERE id=?";
	private static String LAST_INSERT_ID = "SELECT LAST_INSERT_ID();";

	/**********************************************************************************/
	private static String SELECT_ALL_ACCOUNTS_SQL = "SELECT id, user_id, name, balance FROM atm.account";
	private static String SELECT_ACCOUNTS_BY_ID_SQL = "SELECT id, user_id, name, balance FROM atm.account WHERE id=?";
	private static String INSERT_ACCOUNT_SQL = "INSERT INTO  atm.account (user_id,name,balance,last_update) values (?, ?, ?, ?)";
	private static String UPDATE_ACCOUNT_SQL = "UPDATE atm.account SET user_id=?, name=?, balance=?,last_update=? WHERE  id=?";
	/**********************************************************************************/

	private static DataAccess m_dataAccess;
	private static String DEFAULT_PASS = "root";
	private static String DEFAULT_USER = "root";
	private static String CONN_FMT = "jdbc:mysql://localhost/atm?user=%s&password=%s&useSSL=false";
	private String connectionString = "";

	/**
	 * private default constructor
	 * 
	 * @param username
	 *            the name of user
	 * @param userPassword
	 *            the password of user
	 */
	private DataAccess(String m_password, String m_userName) throws AtmDataException {
		this.m_password = m_password;
		this.m_userName = m_userName;
		
		m_formatter = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss");
		AtmLogger.addAtmLoggerHandlers(logger);
		this.connectionString = String.format(CONN_FMT, m_userName, m_password);
		
		connect();
		
		logger.info("Successfully connected to the database " + m_connect.toString());
	}

	public DataAccess() {
	}

	@SuppressWarnings("unused")
	private User updateUser(User user, java.sql.Date updateDate) throws AtmDataException {

		try {
			m_updateUserByIdStatement.setString(1, user.getFirstName());
			m_updateUserByIdStatement.setString(2, user.getLastName());
			m_updateUserByIdStatement.setDate(3, updateDate);
			m_updateUserByIdStatement.setLong(4, user.getUserId());
			m_updateUserByIdStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		}

		return user;
	}

	@SuppressWarnings("unused")
	private User insertUser(User user, java.sql.Date updateDate) throws AtmDataException {

		User newUser = null;

		try {
			m_insertUserStatement.setInt(1, 1234);
			m_insertUserStatement.setString(2, user.getFirstName());
			m_insertUserStatement.setString(3, user.getLastName());
			m_insertUserStatement.setDate(4, updateDate);

			int rowsInserted = m_insertUserStatement.executeUpdate();

			if (rowsInserted > 0) {
				ResultSet rs = m_lastInsertStatement.executeQuery();
				if (rs.next()) {
					int newId = rs.getInt(1);
					newUser = getUserById(newId);
					if (newUser == null) {
						String msg = String.format("Failed to find new inserted user %s %s...", user.getFirstName(),
								user.getLastName());
						throw new AtmDataException(msg);
					}
				}
			} else {
				String msg = String.format("Inserted user %s %s has failed...", user.getFirstName(),
						user.getLastName());
				throw new AtmDataException(msg);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		}

		return newUser;
	}

	public void close() {

		try {
			m_connect.close();
		} catch (SQLException e) {
			trace(e.getMessage());
		}
	}

	public void connect() throws AtmDataException {

		try {
			Class.forName("com.mysql.jdbc.Driver");

			String ConnectionString = String.format(CONN_FMT, m_userName, m_password);

			m_connect = DriverManager.getConnection(ConnectionString);

			m_insertUserStatement = m_connect.prepareStatement(INSERT_USER_SQL);
			m_selectAllUsersStatement = m_connect.prepareStatement(SELECT_USER_SQL);
			m_selectUserByIdStatement = m_connect.prepareStatement(SELECT_USER_BY_ID_SQL);
			m_deleteUserByIdStatement = m_connect.prepareStatement(DELETE_USER_BY_ID_SQL);
			m_updateUserByIdStatement = m_connect.prepareStatement(UPDATE_USER_BY_ID_SQL);
			m_lastInsertStatement = m_connect.prepareStatement(LAST_INSERT_ID);

		} catch (SQLException e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		}

	}

	public boolean deleteUserById(long id) throws AtmDataException {

		boolean result = false;

		try {
			m_deleteUserByIdStatement.setLong(1, id);
			int count = m_deleteUserByIdStatement.executeUpdate();

			result = count == 1 ? true : false;
			trace("delete user row count " + count);

			if (!result) {

			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		}

		return result;
	}

	public Connection getConnection() {
		return m_connect;
	}

	/**
	 * Singleton pattern implementation
	 * 
	 * @return DataAccess instance #throws AtmExeption on error
	 */
	public static DataAccess getInstance() throws AtmDataException {
		return DataAccess.getInstance("root", "root");
	}

	public static DataAccess getInstance(String username, String password) throws AtmDataException {
		if (m_dataAccess == null) {
			m_dataAccess = new DataAccess(username, password);
		}

		return m_dataAccess;
	}

	public User getUserById(long id) throws AtmDataException {

		User user = null;
		ResultSet resultset = null;

		try {
			m_selectUserByIdStatement.setLong(1, id);
			resultset = m_selectUserByIdStatement.executeQuery();

			if (resultset.next()) {
				long userId = resultset.getLong("id");
				String first = resultset.getString("first_name");
				String last = resultset.getString("last_name");
				user = new User(userId, first, last);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		}

		return user;
	}

	public List<User> getUsers() throws AtmDataException {
		List<User> userList = new ArrayList<User>();
		ResultSet resultset = null;

		try {
			resultset = m_selectAllUsersStatement.executeQuery();

			while (resultset.next()) {
				long userId = resultset.getLong("id");
				String first = resultset.getString("first_name");
				String last = resultset.getString("last_name");
				userList.add(new User(userId, first, last));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new AtmDataException(e);
		}
		return userList;
	}

	public User saveUser(User user) throws AtmDataException {
		Calendar now = Calendar.getInstance();

		User updateUser = null;
		Date updateDate = new java.sql.Date(now.getTime().getTime());

		if (user.getUserId() == -1) {
			updateUser = insertUser(user, updateDate);
		} else {
			updateUser = updateUser(user, updateDate);
		}

		return updateUser;
	}

	private void trace(String msg) {
		System.out.println(msg);
	}
}
