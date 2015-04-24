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
import modulo.usuarios.dto.RolUsuarioDto;

public class RolUsuarioDao {

    PreparedStatement pstm = null;
    int rtdo;
    ResultSet rs = null;
    String mensaje = "";
    String sqlTemp = "";

    public String insertRolUsuario(RolUsuarioDto nuevoRolUsuario, Connection unaConexion) {
        try {
            String sqlInsert = "INSERT INTO `rolusuario`(`idRol`, `idUsuario`) VALUES (?, ?)";
            pstm = unaConexion.prepareStatement(sqlInsert);

            pstm.setInt(1, nuevoRolUsuario.getIdRol());
            pstm.setInt(2, nuevoRolUsuario.getIdUsuario());
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
        sqlTemp = "SELECT `idRol`, `descripcion` FROM `roles`";
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
            
            pstm.setString(2, rolActualizado.getDescripcion());
            
            pstm.setInt(4, rolActualizado.getIdRol());
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
    
    public ArrayList<RolDto> obtenerRolesParaUsuario(long documento, Connection unaConexion) {
        String sqlLeerRoles = "SELECT r.idRol, r.descripcion "
                + "FROM roles as r JOIN rolesusuario as ru ON (r.idRol = ru.idRol) "
                + "WHERE ru.idUsuario = ?";
        ArrayList<RolDto> misRoles = new ArrayList();
        try {
            pstm = unaConexion.prepareStatement(sqlLeerRoles);
            pstm.setLong(1, documento);
            rs = pstm.executeQuery();
            while (rs.next()) {
                RolDto tempRol = new RolDto();
                tempRol.setIdRol(rs.getInt("idRol"));                
                tempRol.setDescripcion(rs.getString("descripcion"));                
                misRoles.add(tempRol);
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return misRoles;
    }
}
