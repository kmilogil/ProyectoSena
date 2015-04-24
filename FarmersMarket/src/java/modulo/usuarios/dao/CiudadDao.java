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
import modulo.usuarios.dto.CiudadDto;

public class CiudadDao {

    PreparedStatement pstm = null;
    int rtdo;
    ResultSet rs = null;
    String mensaje = "";
    String sqlTemp = "";    

    public List obtenerCiudadesPorId(int idDepartamento, Connection unaConexion) {
        ArrayList<CiudadDto> ciudades = null;
        sqlTemp = "SELECT `idCiudad`, `Nombre`, `idDepartamento` FROM `ciudades` WHERE idDepartamento = ? ORDER BY `Nombre`";
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            pstm.setInt(1, idDepartamento);
            rs = pstm.executeQuery();

            ciudades = new ArrayList();
            while (rs.next()) {
                CiudadDto temp = new CiudadDto();
                temp.setIdCiudad(rs.getInt("idCiudad"));
                temp.setNombre(rs.getString("Nombre"));
                temp.setIdDepartamento(rs.getInt("idDepartamento"));
                ciudades.add(temp);
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return ciudades;
    }

    public String obtenerNombrePorId(int idCiudad, Connection unaConexion) {
        try {
            String sqlInsert = "SELECT `Nombre`, `idDepartamento` FROM `ciudades` WHERE `idCiudad` = ?";
            pstm = unaConexion.prepareStatement(sqlInsert);

            pstm.setInt(1, idCiudad);
            rs = pstm.executeQuery();

            while (rs.next()) {
                mensaje = rs.getString("Nombre");
            }
        } catch (SQLException sqle) {
            mensaje = "Error, detalle " + sqle.getMessage();
        }
        return mensaje;
    }
}
