package com.bitzcraftonline.whitelist;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

public class SQLConnection
{
  //Attributes
  final String m_strQuery;
  final String m_strQueryAdd;
  final String m_strQueryRemove;
  final String m_strConnection;
  Connection m_Connection;
  Driver m_ProxyDriver;

  public SQLConnection(String strDriver, String strConnection, String strQuery, String strQueryAdd, String strQueryRemove, String strDriverPath,String strUsername, String strPassword) throws Exception
  {
    m_strQuery = strQuery;
    m_strQueryAdd = strQueryAdd;
    m_strQueryRemove = strQueryRemove;
    m_strConnection = strConnection;
    m_Connection = null;
    
    try
    {
      if ( strDriverPath == null || !(new File(strDriverPath).exists()) )
      {
        Class.forName(strDriver).newInstance();
      }
      else
      {
        boolean bUseLoader = true;
        Enumeration<Driver> e = DriverManager.getDrivers();
        while (e.hasMoreElements())
        {
          Driver driver = (Driver) e.nextElement();
          if ( driver.getClass().getName().compareToIgnoreCase(strDriver) == 0 )
            bUseLoader = false;
        }
        if ( bUseLoader )
        {
          URL url = new URL("jar:file:" + strDriverPath + "!/");
          URLClassLoader ucl = new URLClassLoader(new URL[] { url });
          m_ProxyDriver = new DriverProxy((Driver)Class.forName(strDriver, true, ucl).newInstance());
          DriverManager.registerDriver(m_ProxyDriver);
        }
        else
        {
          Class.forName(strDriver).newInstance();
        }
      }
      m_Connection = DriverManager.getConnection(strConnection,strUsername,strPassword);

    }
    catch (SQLException ex)
    {
      System.out.println("Whitelist: SQLException: " + ex.getMessage());
      System.out.println("Whitelist: SQLState: " + ex.getSQLState());
      System.out.println("Whitelist: VendorError: " + ex.getErrorCode());
      throw ex;
    }
    catch (Exception ex)
    {
      System.out.println("Whitelist: Exception: " + ex.toString() + " - missing connector?" );
      throw ex;
    }
  }
  
  public void Cleanup()
  {
    if ( m_ProxyDriver != null )
    {
      try
      {
        DriverManager.deregisterDriver(m_ProxyDriver);
        m_ProxyDriver = null;
      }
      catch ( Exception ex )
      {
        m_ProxyDriver = null;
      }
    }
  }

  public boolean isOnWhitelist(Player player, boolean bRetry)
  {
    try
    {
      if ( m_Connection == null )
        m_Connection = DriverManager.getConnection(m_strConnection);

      
      Statement stmt = m_Connection.createStatement();
      ResultSet rst = stmt.executeQuery(m_strQuery.replace("<%USERNAME%>", player.getName()).replace("<%UUID%>", player.getUniqueId().toString()));
      if ( rst.first() )
        return true;
      else
        return false;
    }
    catch (SQLException ex)
    {
      m_Connection = null;
      if ( bRetry )
      {
        return isOnWhitelist(player, false);
      }
      else
      {
        System.out.println("Whitelist: SQLException: " + ex.getMessage());
        System.out.println("Whitelist: SQLState: " + ex.getSQLState());
        System.out.println("Whitelist: VendorError: " + ex.getErrorCode());
      }
    }
    catch (Exception ex)
    {
      System.out.println("Whitelist: Exception: " +ex.getMessage() );
    }
    return false;
  }

  public boolean addPlayerToWhitelist(Player player, boolean bRetry)
  {
    if ( m_strQueryAdd != null && !m_strQueryAdd.isEmpty() )
    {
      try
      {
        if ( m_Connection == null )
          m_Connection = DriverManager.getConnection(m_strConnection);
        Statement stmt = m_Connection.createStatement();
        stmt.execute(m_strQueryAdd.replace("<%USERNAME%>", player.getName()).replace("<%UUID%>", player.getUniqueId().toString()));
        return true;
      }
      catch (SQLException ex)
      {
        m_Connection = null;
        if ( bRetry )
        {
          return addPlayerToWhitelist(player, false);
        }
        else
        {
          System.out.println("Whitelist: SQLException: " + ex.getMessage());
          System.out.println("Whitelist: SQLState: " + ex.getSQLState());
          System.out.println("Whitelist: VendorError: " + ex.getErrorCode());
        }
      }
      catch (Exception ex)
      {
        System.out.println("Whitelist: Exception: " +ex.getMessage() );
      }
    }
    return false;
  }

  public boolean removePlayerFromWhitelist(Player player, boolean bRetry)
  {
    if ( m_strQueryRemove != null && !m_strQueryRemove.isEmpty() )
    {
      try
      {
        if ( m_Connection == null )
          m_Connection = DriverManager.getConnection(m_strConnection);
        Statement stmt = m_Connection.createStatement();
        stmt.execute(m_strQueryRemove.replace("<%USERNAME%>", player.getName()).replace("<%UUID%>", player.getUniqueId().toString()));
        return true;
      }
      catch (SQLException ex)
      {
        m_Connection = null;
        if ( bRetry )
        {
          return removePlayerFromWhitelist(player, false);
        }
        else
        {
          System.out.println("Whitelist: SQLException: " + ex.getMessage());
          System.out.println("Whitelist: SQLState: " + ex.getSQLState());
          System.out.println("Whitelist: VendorError: " + ex.getErrorCode());
        }
      }
      catch (Exception ex)
      {
        System.out.println("Whitelist: Exception: " +ex.getMessage() );
      }
    }
    return false;
  }
}
