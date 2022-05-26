#include "person.h"
#include "database.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <sstream>
#include <exception>
#include <fstream>

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{
    
    void Person::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();
            //*
            Statement drop_stmt(session);
            drop_stmt << "DROP TABLE IF EXISTS Person", now;
            //*/

            // (re)create table

            for (auto &hint : database::Database::get_all_hints())
            {
                Statement create_stmt(session);
                create_stmt << "CREATE TABLE IF NOT EXISTS `Person` (`login` VARCHAR(50) NOT NULL,"
                            << "`first_name` VARCHAR(256),"
                            << "`last_name` VARCHAR(256),"
                            << "`age` INT ,"
                            << "CONSTRAINT pk_corporation PRIMARY KEY (`login`));"
                            << hint,
                    now;
            }
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }
    /*
    void Person::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();
            
            Statement drop_stmt(session);
            drop_stmt << "DROP TABLE IF EXISTS Person", now;
            

            // (re)create table
            Statement create_stmt(session);
            create_stmt << "CREATE TABLE IF NOT EXISTS `Person` (`login` VARCHAR(50) NOT NULL,"
                        << "`first_name` VARCHAR(256),"
                        << "`last_name` VARCHAR(256),"
                        << "`age` INT ,"
                        << "CONSTRAINT pk_corporation PRIMARY KEY (`login`));",
                now;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }
    */
    void Person::preload(const std::string &file)
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();
            std::string json;
            std::ifstream is(file);
            std::istream_iterator<char> eos;
            std::istream_iterator<char> iit(is);
            while (iit != eos)
                json.push_back(*(iit++));
            is.close();

            Poco::JSON::Parser parser;
            Poco::Dynamic::Var result = parser.parse(json);
            Poco::JSON::Array::Ptr arr = result.extract<Poco::JSON::Array::Ptr>();

            size_t i{0};
            for (i = 0; i < arr->size(); ++i)
            {
                Poco::JSON::Object::Ptr object = arr->getObject(i);
                std::string login = object->getValue<std::string>("login");
                std::string first_name = object->getValue<std::string>("first_name");
                std::string last_name = object->getValue<std::string>("last_name");
                long age = object->getValue<long>("age");
                Poco::Data::Statement insert(session);
                insert << "INSERT INTO Person (Login,first_name,last_name,age) VALUES(?, ?, ?, ?)",
                    Poco::Data::Keywords::use(login),
                    Poco::Data::Keywords::use(first_name),
                    Poco::Data::Keywords::use(last_name),
                    Poco::Data::Keywords::use(age);
                   
                insert.execute();
            }

            std::cout << "Inserted " << i << " records" << std::endl;
        }
        
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    Poco::JSON::Object::Ptr Person::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("login", _login);
        root->set("first_name", _first_name);
        root->set("last_name", _last_name);
        root->set("age", _age);

        return root;
    }

    Person Person::fromJSON(const std::string &str)
    {
        Person person;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        person.login() = object->getValue<std::string>("login");
        person.first_name() = object->getValue<std::string>("first_name");
        person.last_name() = object->getValue<std::string>("last_name");
        person.age() = object->getValue<long>("age");

        return person;
    }

    Person Person::read_by_login(std::string login)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            Person p;
            std::string sharding_hint = database::Database::sharding_hint(login);

            std::string select_str = "SELECT login, first_name, last_name, age FROM Person where login=?";
            select_str += sharding_hint;

            select << select_str,
                into(p._login),
                into(p._first_name),
                into(p._last_name),
                into(p._age),
                use(login),
                range(0, 1); //  iterate over result set one row at a time
  
            select.execute();
            Poco::Data::RecordSet rs(select);
            if (!rs.moveFirst()) throw std::logic_error("not found");

            return p;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::read_all()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<Person> result;
            Person p;
            std::string string = "string";
            std::string sharding_hint = database::Database::sharding_hint(string);
            std::string select_str = "SELECT login, first_name, last_name, age FROM Person";
            select_str += sharding_hint;

            select << select_str,
                into(p._login),
                into(p._first_name),
                into(p._last_name),
                into(p._age),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done())
            {
                if(select.execute())
                result.push_back(p);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::search(std::string first_name, std::string last_name)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<Person> result;
            Person p;
            first_name+="%";
            last_name+="%";
            select << "SELECT login, first_name, last_name, age FROM Person where first_name LIKE ? and last_name LIKE ?",
                into(p._login),
                into(p._first_name),
                into(p._last_name),
                into(p._age),
                use(first_name),
                use(last_name),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done())
            {
                if(select.execute())  result.push_back(p);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

   
    void Person::save_to_mysql()
    {

        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);
            std::string sharding_hint = database::Database::sharding_hint(_login);

            std::string select_str = "INSERT INTO Person (login,first_name,last_name,age) VALUES(?, ?, ?, ?)";
            select_str += sharding_hint;

            insert << select_str,
                use(_login),
                use(_first_name),
                use(_last_name),
                use(_age);

            insert.execute();

            std::cout << "inserted:" << _login << std::endl;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }


    const std::string &Person::get_login() const
    {
        return _login;
    }

    const std::string &Person::get_first_name() const
    {
        return _first_name;
    }

    const std::string &Person::get_last_name() const
    {
        return _last_name;
    }

    long Person::get_age() const
    {
        return _age;
    }

    std::string &Person::login()
    {
        return _login;
    }

    std::string &Person::first_name()
    {
        return _first_name;
    }

    std::string &Person::last_name()
    {
        return _last_name;
    }

    long &Person::age()
    {
        return _age;
    }
}