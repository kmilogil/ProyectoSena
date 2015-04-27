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
import modulo.ofertas.dto.PromocionDto;

/**
 *
 * @author User
 */
public class PromocionDao {
    String mensaje="";
    PreparedStatement ps = null;
    ResultSet rs = null;
    
    
    public List obtenerPromociones(Connection unaConexion){
        ArrayList<PromocionDto> promociones = new ArrayList();
        try {
            String query = "Select idPromocion, descripcion, detalle from promociones;";
            ps = unaConexion.prepareStatement(query);
            rs = ps.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    PromocionDto promocion = new PromocionDto();
                    promocion.setIdPromocion(rs.getInt("idPromocion"));
                    promocion.setDescripcion(rs.getString("descripcion"));
                    promocion.setDetalle(rs.getFloat("detalle"));
                    promociones.add(promocion);

                }
            }
        } catch (SQLException ex) {
            mensaje = "Error" + ex.getSQLState() + " - " + ex.getMessage();
        }
        
        return promociones;
        
    }
}
