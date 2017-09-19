package week04.util;

/**
 * @author Eric Willoughby
 * @since 9/18/17
 * */

import java.io.IOException;
import java.util.logging.*;

public class AtmLogger {

	static private FileHandler fileTxt;
	static private SimpleFormatter formatterTxt;
	static private FileHandler fileHTML;
	static private Formatter formatterHTML;

	static public String ATM_LOGGER = "ATM_LOGGER";
	static Logger atmLogger;

	/**
	 * Static initializer
	 */
	static {
		atmLogger = Logger.getLogger(ATM_LOGGER);
	}

	/**
	 * returns the singleton instance of the logger
	 * 
	 * @return
	 */
	static Logger getAtmLogger() {
		return atmLogger;
	}

	/**
	 * Initializes the logging system for our purposes
	 * 
	 * @throws IOException
	 */
	static public void setup() throws IOException {
		// Suppress the logging output to the console
		Logger rootLogger = Logger.getLogger("");
		Handler[] handlers = rootLogger.getHandlers();

		if (handlers.length > 0) {
			rootLogger.removeHandler(handlers[0]);
		}

		// set log level and names
		atmLogger.setLevel(Level.INFO);
		fileTxt = new FileHandler("Logging.txt");
		fileHTML = new FileHandler("Logging.html");

		// create text formatter
		formatterTxt = new SimpleFormatter();
		fileTxt.setFormatter(formatterTxt);
		atmLogger.addHandler(fileTxt);

		// create html formatter
		formatterHTML = new AtmHtmlLoggingFormatter();
		fileHTML.setFormatter(formatterHTML);
		atmLogger.addHandler(fileHTML);
	}
	
	/**
	 * @param Logger
	 * */
	public static void addAtmLoggerHandlers(Logger logger){
		logger.addHandler(fileTxt);
		logger.addHandler(fileHTML);
	}
}

















