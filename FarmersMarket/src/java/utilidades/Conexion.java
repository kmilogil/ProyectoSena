/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utilidades;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author User
 */
public class Conexion {

    private static Connection conexion = null;

    private static void conectar() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conexion = DriverManager.getConnection("jdbc:mysql://localhost/farm2015", "root", "");
        } catch (SQLException e) {
            System.out.println("Error de MySQL: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("Error inesperado: " + e.getMessage());
        }
    }

    private Conexion() {

    }

    public static Connection getInstance() {
        if (conexion == null) {
            conectar();
        }
        return conexion;
    }
}
