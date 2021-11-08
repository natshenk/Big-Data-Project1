package P1

import java.io.IOException

import scala.util.Try

// Hive
import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import scala.io.Source
import net.liftweb.json._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Project1 {
    // Global login variables
    var userLoggedin = false
    var adminLoggedin = false
    var currUser: String = null

    def main(args: Array[String]): Unit = {
        giveWelcomeOptions()
    }

    // Welcomes the user and handles their choice of creating an account,
    // logging in, or exiting the application
    def giveWelcomeOptions(): Unit = {
        while (!userLoggedin && !adminLoggedin) {
            println(
                "\nWelcome to Fantasy Football News Tracker!\n" +
                "Please log in to your account or create one if you're new by\n" +
                "selecting an option from the list below:\n" +
                "1 : Create an account\n" +
                "2 : Login\n" +
                "3 : Exit"
            )

            val userSelection = readLine("Enter your selection # > ")

            userSelection match {
                case "1" =>
                    addAccount()
                case "2" =>
                    login()
                case "3" => 
                    println("Thanks for using our application!")
                    Thread.sleep(1500)
                    return
                case _ => 
                    println("Please enter a valid option")
                    Thread.sleep(1500)
            }
        }

        // Once the user is logged in, give them options based on their admin status
        if (userLoggedin) {
            giveBasicOptions()
        } else if (adminLoggedin) {
            giveAdminOptions()
        } else {
            println("Unexpected error during authentication. Application closing")
            Thread.sleep(3000)
        }
    }

    // Prompts user for info to create an account and adds them to the
    // users table in Hive
    def addAccount(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            val fName = readLine("\nWhat is your first name? > ")
            val lName = readLine("What is your last name? > ")

            // Get all usernames that currently exist
            val sql = "SELECT Username FROM users"
            val usernames = stmt.executeQuery(sql);

            var username = readLine("Please enter a username > ")
            var usernameIsUnique = true

            // Check the entered username against all existing ones
            while (usernames.next()) {
                var userCheck = usernames.getString(1)
                if (userCheck == username || username == "exit") {
                    usernameIsUnique = false
                }
            }

            // If the username was not unique, keep prompting the user until a unique
            // username is entered
            while (!usernameIsUnique) {
                println("Username already exists. Try again.")
                username = readLine("Please enter a username > ")
                usernameIsUnique = true
                while (usernames.next()) {
                    if (usernames.getString(1) == username) {
                        usernameIsUnique = false
                    }
                }
            }
            
            // Prompt user for a password and verify it by asking them to re-enter it
            var password: String = " "
            var confirmPass: String = ""
            while (password != confirmPass) {
                password = readLine("Please enter a password > ")
                confirmPass = readLine("Re-enter your password > ")
                if (password != confirmPass) {
                    println("First password does not match the second. Please try again")
                }
            }

            println("Creating account, please wait...")

            // Add the user to the users table with their provided info
            val sql2 = s"INSERT INTO users VALUES ('$fName', '$lName', '$username', '$password', false)"
            stmt.execute(sql2)

            println(s"Success! Account '$username' was created. Redirecting to menu...")
            Thread.sleep(2000)
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    // Prompts the user for their username and password and checks it with
    // the existing accounts in Hive. Continues to prompt the user
    // if their information does not match an account and allows
    // the user to return to the main menu
    def login(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            var loginSuccess = false
            while (!loginSuccess) {
                val username = readLine("\nPlease enter your username, or type 'exit' to quit > ")
                if (username == "exit") {
                    println("Returning to main menu...")
                    Thread.sleep(1000)
                    return
                }
                val password = readLine("Please enter your password > ")
                
                val pwSql = s"SELECT Password FROM users WHERE username = '$username'"
                val pwResults = stmt.executeQuery(pwSql)

                var correctPass = ""
                while (pwResults.next()) {
                    correctPass += pwResults.getString(1)
                }

                // If the correct password was entered, check if the user is an admin
                if (password == correctPass) {
                    val adminSql = s"SELECT isAdmin FROM users WHERE username = '$username'"
                    val adminResults = stmt.executeQuery(adminSql)

                    var isAdmin = false
                    while (adminResults.next()) {
                        isAdmin = adminResults.getBoolean(1)
                    }

                    if (isAdmin) {
                        adminLoggedin = true
                    } else {
                        userLoggedin = true
                    }
                    currUser = username
                    loginSuccess = true
                    return
                } else {
                    println("Incorrect username or password. Please try again")
                    Thread.sleep(2000)
                }
            }
        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    def giveBasicOptions(): Unit = {
        var exit = false
        while (!exit) {
            println(
                s"\nWelcome $currUser! Please select what you would like to do:\n" +
                "1 : View top fantasy football news for this week\n" +
                "2 : View top fantasy football news for next week\n" +
                "3 : View NFL injury news\n" +
                "4 : Change first/last name\n" +
                "5 : Change password\n" +
                "6 : Delete your account\n" +
                "7 : Logout\n"
            )

            val userSelection = readLine("Enter your selection # > ")

            userSelection match {
                case "1" =>
                    viewCurrentNews()
                case "2" =>
                    viewNextWeekNews()
                case "3" =>
                    viewInjuryNews()
                case "4" =>
                    changeAccName()
                case "5" =>
                    changePassword()
                case "6" =>
                    val deleted = deleteAccount()
                    if (deleted) {
                        giveWelcomeOptions()
                        return
                    }
                case "7" => 
                    println("See you next time!")

                    currUser = null
                    userLoggedin = false
                    adminLoggedin = false

                    Thread.sleep(1500)

                    giveWelcomeOptions()

                    return
                case _ => 
                    println("Please enter a valid option")
                    Thread.sleep(1500)
            }
        }
    }

    // Asks the user which part of their name they want to change, then
    // executes the necessary HiveQL query to change the user's row in the users table
    def changeAccName(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            // Ask user for which name to change
            println(
                "\nSelect which name to change:\n" +
                "1 : First name\n" +
                "2 : Last name\n" +
                "3 : Go back"
            )

            val userSelection = readLine("Enter your selection # > ")
            
            userSelection match {
                case "1" =>
                    val newFname = readLine("Please enter a new first name for your account > ")

                    println("Updating account info...")

                    val sql = s"UPDATE users SET first_name = '$newFname' WHERE username = '$currUser'"
                    stmt.execute(sql)
                case "2" =>
                    val newLname = readLine("Please enter a new last name for your account > ")

                    println("Updating account info...")

                    val sql = s"UPDATE users SET last_name = '$newLname' WHERE username = '$currUser'"
                    stmt.execute(sql)
                case "3" => 
                    println("Returning to user menu...")
                    Thread.sleep(1500)
                    return
            }
        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    // Confirms user's old password before asking for a new one (with a confirmation)
    // then executes the appropriate HiveQL query
    def changePassword(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            val oldPass = readLine("\nPlease enter your current password > ")

            val pwSql = s"SELECT Password FROM users WHERE username = '$currUser'"
            val pwResults = stmt.executeQuery(pwSql)

            var correctPass = ""
            while (pwResults.next()) {
                correctPass += pwResults.getString(1)
            }

            // If the correct password was entered, ask for the new password
            if (oldPass == correctPass) {
                var pwUpdatedSuccessfully = false

                // Enforce a password re-entry confirmation
                while (!pwUpdatedSuccessfully) {
                    val newPass = readLine("Please enter a new password for your account > ")
                    val confirmPass = readLine("Re-enter your new password > ")
                    
                    if (newPass == confirmPass) {
                        println("Updating password...")

                        val sql = s"UPDATE users SET password = '$newPass' WHERE username = '$currUser'"
                        stmt.execute(sql)

                        pwUpdatedSuccessfully = true
                    } else {
                        println("Re-entered password does not match. Please try again.")
                        Thread.sleep(1500)
                    }
                }
            } else {
                println("Password incorrect. Returning to user menu...")
                Thread.sleep(1500)
            }
        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    // Confirms the user's account deletion request and executes the necessary query
    // Returns whether the account was deleted or not
    def deleteAccount(): Boolean = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            val confirm = readLine("\nAre you sure you want to delete your account? (y/n) > ")

            if (confirm == "y") {
                println("Deleting your account. We're sorry to see you go!\n" +
                        "Feel free to make a new account at any time!")

                val sql = s"DELETE FROM users WHERE username = '$currUser'"
                stmt.execute(sql)

                return true
            } else {
                println("Account deletion canceled. Returning to user menu...")
                Thread.sleep(1500)
                return false
            }
        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    // Provides admin users with basic user options plus addtional admin options
    // Admin accounts are responsible for updating the application with current
    // info from the News API
    def giveAdminOptions(): Unit = {
        var exit = false
        while (!exit) {
            println(
                s"\nWelcome $currUser! Please select what you would like to do:\n" +
                "1 : View top fantasy football news for this week\n" +
                "2 : View top fantasy football news for next week\n" +
                "3 : View NFL injury news\n" +
                "4 : Change first/last name\n" +
                "5 : Change password\n" +
                "6 : Delete your account\n" +
                "7 : Logout\n" +
                "------- ADMIN OPTIONS -------\n" +
                "8 : Load current news into application\n" +
                "9 : Give a basic user admin priveleges\n" +
                "10 : Delete an account"
            )

            val userSelection = readLine("Enter your selection # > ")

            userSelection match {
                case "1" =>
                    viewCurrentNews()
                case "2" =>
                    viewNextWeekNews()
                case "3" =>
                    viewInjuryNews()
                case "4" =>
                    changeAccName()
                case "5" =>
                    changePassword()
                case "6" =>
                    val deleted = deleteAccount()
                    if (deleted) {
                        giveWelcomeOptions()
                        return
                    }
                case "7" => 
                    println("See you next time!")

                    currUser = null
                    userLoggedin = false
                    adminLoggedin = false

                    Thread.sleep(1500)

                    giveWelcomeOptions()

                    return
                case "8" =>
                    getTopNews()
                    // TODO
                    // Load current news into application
                case "9" =>
                    promoteUser()
                case "10" =>
                    deleteUser()
                case _ => 
                    println("Please enter a valid option")
                    Thread.sleep(1500)
            }
        }
    }

    // Prompts the admin for a basic user that will be promoted to an admin then
    // applies the change in Hive
    def promoteUser(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            val basicUser = readLine("\nEnter the username of the basic user to promote > ")

            val adminSql = s"SELECT isAdmin FROM users WHERE username = '$basicUser'"
            val adminResults = stmt.executeQuery(adminSql)

            var isAdmin = false
            while (adminResults.next()) {
                isAdmin = adminResults.getBoolean(1)
            }

            if (isAdmin) {
                println("User is already an admin. Returning to menu...")
                Thread.sleep(1500)
                return
            } else {
                println("Changing user's permissions...")

                val promoteSql = s"UPDATE users SET isAdmin = true WHERE username = '$basicUser'"
                stmt.execute(promoteSql)
            }

        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    // Deletes a specified account from Hive
    def deleteUser(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            val userToDelete = readLine("\nWhich account do you want to delete? > ")

            val adminSql = s"SELECT isAdmin FROM users WHERE username = '$userToDelete'"
            val adminResults = stmt.executeQuery(adminSql)

            var isAdmin = false
            while (adminResults.next()) {
                isAdmin = adminResults.getBoolean(1)
            }

            if (isAdmin) {
                println("Cannot delete admin account. Consult the database manager.")
                Thread.sleep(1500)
                return
            }

            val confirm = readLine(s"Are you sure you want to delete the account $userToDelete? (y/n) > ")

            if (confirm == "y") {
                println("Deleting account...")

                val sql = s"DELETE FROM users WHERE username = '$userToDelete'"
                stmt.execute(sql)

                return true
            } else {
                println("Account deletion canceled. Returning to admin menu...")
                Thread.sleep(1500)
                return false
            }
        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    //****************************************************************
    // API fetch and query functions
    //****************************************************************

    def getTopNews(): Unit = {
        var con: java.sql.Connection = null
        try {
            // For Hive2:
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/fantasy_sports"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()

            // For Acid writes
            stmt.execute("set hive.enforce.bucketing=true")

            val currDate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)

            val url = "https://newsapi.org/v2/everything?q=fantasy%20football%20-college&from=" + currDate + "&sortBy=popularity&apiKey=2f6ad7c3df9b4b7383e31ed28154a3c3"

            val apiResults = scala.io.Source.fromURL(url).mkString

            case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
            case class Source (id: String, name: String)
            case class apiResponse (status: String, totalResults: Int, articles: Array[Article])

            implicit val formats = DefaultFormats

            val deleteSql = "DROP TABLE IF EXISTS news"
            stmt.execute(deleteSql)

            val createSql = "CREATE EXTERNAL TABLE news(source string, author string, title string, description string, url string, urlToImage string, publishedAt string, content string) " +
              "CLUSTERED BY (title) INTO 3 buckets " +
              "row format delimited " +
              "fields terminated by ',' " +
              "stored as orc"
            stmt.execute(createSql)

            val json = parse(apiResults)
            val res = json.extract[apiResponse]
            //val articles = res.articles.extract[Article]
            for (article <- res.articles) {
                /*
                val insertSql = "INSERT INTO news VALUES ('" + article.source.toString + "', '" + article.author + "', '" + 
                    article.title + "', '" + article.description + "', '" + article.url + "', '" + 
                    article.urlToImage + "', '" + article.publishedAt + "', '" + article.content + "')"
                stmt.execute(insertSql)
                */
            }

            Thread.sleep(3000)
            println("News fetched")

        // Catch Hive exceptions
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch {
                case ex => {
                    ex.printStackTrace();
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    // Replaces HTML tags and extraneous punctuation
    def cleanString(input: String): String = {
        return input.replace("<ol>", "").replace("</ol>", " ").replace("</li>"," ").replace("<li>", "").replace("<b>", "").replace("</b>", "").replace(",", "").replace(".", "").replace("\n", " ").replace("\t", " ").replace("’", " ").replace("'", "").replace("\"", "").replace("!", "").replace("?", "").replace("`", "").replace(";", "").replace(":", "").replace("(", "").replace(")", "").trim()
    }

    def viewCurrentNews(): Unit = {
        val currDate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)

        val url = "https://newsapi.org/v2/everything?q=fantasy%20football%20-college&from=" + currDate + "&sortBy=popularity&apiKey=2f6ad7c3df9b4b7383e31ed28154a3c3"

        val apiResults = scala.io.Source.fromURL(url).mkString

        case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
        case class Source (id: String, name: String)
        case class apiResponse (status: String, totalResults: Int, articles: Array[Article])

        implicit val formats = DefaultFormats

        val json = parse(apiResults)
        val res = json.extract[apiResponse]

        for (article <- res.articles) {
            println("\nArticle Title: " + cleanString(article.title))
            println("Article description: " + cleanString(article.description))
            println("URL: " + article.url + "\n")
        }
    }

    def viewNextWeekNews(): Unit = {
        val currDate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)

        val url = "https://newsapi.org/v2/everything?q=NFL%20\"week%2010\"%20-college&from=" + currDate + "&sortBy=popularity&apiKey=2f6ad7c3df9b4b7383e31ed28154a3c3"
        
        val apiResults = scala.io.Source.fromURL(url).mkString

        case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
        case class Source (id: String, name: String)
        case class apiResponse (status: String, totalResults: Int, articles: Array[Article])

        implicit val formats = DefaultFormats

        val json = parse(apiResults)
        val res = json.extract[apiResponse]

        for (article <- res.articles) {
            println("\nArticle Title: " + cleanString(article.title))
            println("Article description: " + cleanString(article.description))
            println("URL: " + article.url + "\n")
        }
    }

    def viewInjuryNews(): Unit = {
        val currDate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)

        val url = "https://newsapi.org/v2/everything?q=NFL%20injury%20hurt&from=" + currDate + "&sortBy=popularity&apiKey=2f6ad7c3df9b4b7383e31ed28154a3c3"
    
        val apiResults = scala.io.Source.fromURL(url).mkString

        case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
        case class Source (id: String, name: String)
        case class apiResponse (status: String, totalResults: Int, articles: Array[Article])

        implicit val formats = DefaultFormats

        val json = parse(apiResults)
        val res = json.extract[apiResponse]

        for (article <- res.articles) {
            println("\nArticle Title: " + cleanString(article.title))
            println("Article description: " + cleanString(article.description))
            println("URL: " + article.url + "\n")
        }
    }
}