/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.ofertas.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author kennross
 */
public class CategoriaDao {
    
    PreparedStatement pstm = null;
    int rtdo;
    ResultSet rs = null;
    String mensaje = "";
    String sqlTemp = "";


    public String obtenerNombrePorId(int idCategoria, Connection unaConexion) {
        sqlTemp = "SELECT `descripcion` FROM `categorias` WHERE `idCategoria` = ?";
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            pstm.setInt(1, idCategoria);
            rs = pstm.executeQuery();

            while (rs.next()) {
                mensaje = rs.getString("descripcion");
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return mensaje;
    }

}
