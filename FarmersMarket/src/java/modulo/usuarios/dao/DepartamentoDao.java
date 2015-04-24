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
import modulo.usuarios.dto.DepartamentoDto;


public class DepartamentoDao {

    
    PreparedStatement pstm = null;
    int rtdo;
    ResultSet rs = null;
    String mensaje = "";
    String sqlTemp = "";

    public List obtenerDepartamentos(Connection unaConexion) {
        ArrayList<DepartamentoDto> departamentos = null;
        sqlTemp = "SELECT `idDepartamento`, `departamento` FROM `departamentos` ORDER BY `departamento`";
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            rs = pstm.executeQuery();

            departamentos = new ArrayList();
            while (rs.next()) {
                DepartamentoDto temp = new DepartamentoDto();
                temp.setIdDepartamento(rs.getInt("idDepartamento"));
                temp.setDepartamento(rs.getString("departamento"));
                departamentos.add(temp);
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return departamentos;
    }

    public String obtenerNombrePorId(int idDepartamento, Connection unaConexion) {
        try {
            String sqlInsert = "SELECT `departamento` FROM `departamentos` WHERE `idDepartamento` = ?";
            pstm = unaConexion.prepareStatement(sqlInsert);

            pstm.setInt(1, idDepartamento);
            rs = pstm.executeQuery();

            while (rs.next()) {
                mensaje = rs.getString("departamento");
            }
        } catch (SQLException sqle) {
            mensaje = "Error, detalle " + sqle.getMessage();
        }
        return mensaje;
    }
}
