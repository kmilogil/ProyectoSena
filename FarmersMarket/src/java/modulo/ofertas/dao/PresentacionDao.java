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
import java.util.ArrayList;
import java.util.List;
import modulo.ofertas.dto.PresentacionDto;

/**
 *
 * @author User
 */
public class PresentacionDao {

    String mensaje = "";
    PreparedStatement ps = null;
    ResultSet rs = null;

    public List obtenerPresentaciones(int idCategoria,Connection unaConexion) {
        ArrayList<PresentacionDto> presentaciones = new ArrayList();
        try {
            String query = "select pre.idPresentacion,pre.descripcion from presentaciones as pre "
                    + "join categoriapresentacion as capre on pre.idPresentacion = capre.idPresentacion "
                    + "join categorias as ca on ca.idCategoria = capre.idCategoria where ca.idCategoria = ?;";
            ps = unaConexion.prepareStatement(query);
            ps.setInt(1, idCategoria);
            rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    PresentacionDto presentacion = new PresentacionDto();
                    presentacion.setIdPresentacion(rs.getInt("idPresentacion"));
                    presentacion.setDescripcion(rs.getString("descripcion"));
                    presentaciones.add(presentacion);
                }
            }
        } catch (SQLException ex) {
            mensaje = "Error" + ex.getSQLState() + " - " + ex.getMessage();
        }

        return presentaciones;
    }
}
