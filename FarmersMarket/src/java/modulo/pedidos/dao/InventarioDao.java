/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.pedidos.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import modulo.pedidos.dto.CarritoDto;

/**
 *
 * @author User
 */
public class InventarioDao {

    PreparedStatement pstm = null;
    ResultSet rs = null;
    CallableStatement ctm = null;
    int rtdo = 0;
    String salida = "";

    public String descontarInventario(ArrayList<CarritoDto> a, Connection unaConexion) {
        try {
            pstm = unaConexion.prepareStatement("update inventario set cantidad = cantidad - ? where idOferta = ?;");
            for (int i = 0; i < a.size(); i++) {
                pstm.setLong(1, a.get(i).getCantidad());
                pstm.setInt(2, a.get(i).getIdOferta());
                rtdo = pstm.executeUpdate();
            }

            if (rtdo != 0) {
                salida = "Descontado";
            } else {
                salida = "No";
            }

        } catch (SQLException ex) {
            salida = ex.getMessage();
        }
        return salida;
    }

    public String actualizarCantidad(int cantidad, int idOferta, Connection unaConexion) {
        try {
            pstm = unaConexion.prepareStatement("update inventario set cantidad = ? where idOferta = ?;");
            pstm.setInt(1, cantidad);
            pstm.setInt(2, idOferta);
            rtdo = pstm.executeUpdate();
            if (rtdo != 0) {
                salida = "ok";
            } else {
                salida = "no";
            }
        } catch (SQLException ex) {
            salida = ex.getMessage();
        }
        return salida;
    }

    public String abastecerInventario(int cantidad, int idOferta, Connection unaConexion) {
        try {
            pstm = unaConexion.prepareStatement("UPDATE inventario SET cantidad = cantidad + ? WHERE idOferta = ?");
            pstm.setInt(1, cantidad);
            pstm.setInt(2, idOferta);
            rtdo = pstm.executeUpdate();
            if (rtdo != 0) {
                salida = "ok";
            } else {
                salida = "no";
            }
        } catch (SQLException ex) {
            salida = ex.getMessage();
        }
        return salida;
    }

    public String cancelarPedido(int idPedido, ArrayList<CarritoDto> a, Connection unaConexion) {
        try {
            ctm = unaConexion.prepareCall("call sp_cancelarPedido(?,?,?)");
            for (int i = 0; i < a.size(); i++) {
                ctm.setLong(1, idPedido);
                ctm.setInt(2, a.get(i).getIdOferta());
                ctm.registerOutParameter(3, Types.INTEGER);
                ctm.execute();
            }
            rtdo = ctm.getInt(3);

            if (rtdo != 0) {
                salida = "ok";
            } else {
                salida = "no";
            }

        } catch (SQLException ex) {
            salida = ex.getMessage();
        }
        return salida;
    }

}
