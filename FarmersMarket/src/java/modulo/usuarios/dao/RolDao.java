package modulo.usuarios.dao;
/**
 * @author Kennit David Ruz Romero 
 * Fecha: 4 de de Enero de 2015
 */

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import modulo.usuarios.dto.RolDto;

public class RolDao {

    PreparedStatement pstm = null;
    int rtdo;
    ResultSet rs = null;
    String mensaje = "";
    String sqlTemp = "";

    public String insertRol(RolDto nuevoRol, Connection unaConexion) {
        try {
            String sqlInsert = "INSERT INTO `roles` VALUES (null, ?)";
            pstm = unaConexion.prepareStatement(sqlInsert);

            pstm.setString(1, nuevoRol.getDescripcion());            
            
            rtdo = pstm.executeUpdate();

            if (rtdo != 0) {
                mensaje = "ok";
            } else {
                mensaje = "okno";
            }
        } catch (SQLException sqle) {
            mensaje = "Error, detalle " + sqle.getMessage();
        }
        return mensaje;
    }

    public List obtenerRoles(Connection unaConexion) {
        ArrayList<RolDto> roles = null;
        sqlTemp = "SELECT `idRol``Descripcion` FROM `roles`";
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            rs = pstm.executeQuery();

            roles = new ArrayList();
            while (rs.next()) {
                RolDto temp = new RolDto();
                temp.setIdRol(rs.getInt("idRol"));                
                temp.setDescripcion(rs.getString("descripcion"));                
                roles.add(temp);
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return roles;
    }

    public String actualizarRol(RolDto rolActualizado, Connection unaConexion) {
        try {
            String sqlInsert = "UPDATE `roles` SET `descripcion` = ? WHERE `idRol` = ?";
            pstm = unaConexion.prepareStatement(sqlInsert);
            
            pstm.setString(1, rolActualizado.getDescripcion());            
            pstm.setInt(2, rolActualizado.getIdRol());
            rtdo = pstm.executeUpdate();

            if (rtdo != 0) {
                mensaje = "ok";
            } else {
                mensaje = "okno";
            }
        } catch (SQLException sqle) {
            mensaje = "Error, detalle " + sqle.getMessage();
        }
        return mensaje;
    }    
}
