package modulo.usuarios;

import modulo.usuarios.dao.CiudadDao;
import modulo.usuarios.dao.DepartamentoDao;
import modulo.usuarios.dao.PermisoDao;
import modulo.usuarios.dao.PermisoRolDao;
import modulo.usuarios.dao.RolDao;
import modulo.usuarios.dao.RolUsuarioDao;
import modulo.usuarios.dao.UsuarioDao;
import modulo.usuarios.dto.CiudadDto;
import modulo.usuarios.dto.DepartamentoDto;
import modulo.usuarios.dto.PermisoDto;
import modulo.usuarios.dto.PermisoRolDto;
import modulo.usuarios.dto.RolDto;
import modulo.usuarios.dto.RolUsuarioDto;
import modulo.usuarios.dto.UsuarioDto;
import utilidades.Conexion;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kennross
 */
public class FUsuario {

    Connection miConexion = null;

    //Clases DataAccessObject (DAO)
    CiudadDao ciDao = null;
    DepartamentoDao depDao = null;
    PermisoDao perDao = null;
    PermisoRolDao perRolDao = null;
    RolDao rlDao = null;
    RolUsuarioDao rlUsuDao = null;
    UsuarioDao uDao = null;

    //Clases DataTransferObject (DTO)
    CiudadDto ciDto = null;
    DepartamentoDto depDto = null;
    PermisoDto perDto = null;
    PermisoRolDto perRolDto = null;
    RolDto rlDto = null;
    RolUsuarioDto rlUsuDto = null;
    UsuarioDto uDto = null;

    public FUsuario() {
        //Instancio la conexion
        this.miConexion = Conexion.getInstance();

        this.ciDao = new CiudadDao();
        this.depDao = new DepartamentoDao();
        this.perDao = new PermisoDao();
        this.perRolDao = new PermisoRolDao();
        this.rlDao = new RolDao();
        this.rlUsuDao = new RolUsuarioDao();
        this.uDao = new UsuarioDao();
        this.ciDto = new CiudadDto();
        this.depDto = new DepartamentoDto();
        this.perDto = new PermisoDto();
        this.perRolDto = new PermisoRolDto();
        this.rlDto = new RolDto();
        this.rlUsuDto = new RolUsuarioDto();
        this.uDto = new UsuarioDto();

    }

    //Inserts a la base de datos directamente (Usuario)
    public String registrarUsuario(UsuarioDto nuevoUsuario, RolDto suRol) {
        return uDao.insertUsuarioProcedimiento(nuevoUsuario, suRol, miConexion);
    }

    //Actualizaciones realizadas a la base de datos (Usuario)
    public String cambiarContrasena(String nuevaClave, long documento) {
        return uDao.actualizarClave(nuevaClave, documento, miConexion);
    }

    //Consultas Por Un Criterio (Usuario) 
    public String obtenerCorreoPorDocumento(long documento) {
        return uDao.obtenerCorreoPorId(documento, miConexion);
    }

    public StringBuilder validarExistenciaDocumento(long documento) {
        return uDao.validarExistenciaDocumento(documento, miConexion);
    }

    public StringBuilder validarExistenciaCorreo(String correo) {
        return uDao.validarExistenciaCorreo(correo, miConexion);
    }

    public UsuarioDto validarLaSesion(long documento) {
        return uDao.validarSesion(documento, miConexion);
    }

    public UsuarioDto obtenerUsuarioPorCorreo(String correo) {
        return uDao.obtenerUsuarioPorCorreo(correo, miConexion);
    }

    public UsuarioDto obtenerUsuarioPorDocumento(long documento) {
        return uDao.obtenerUsuarioPorId(documento, miConexion);
    }

    //Consultas Por Criterio (Roles y Permisos)
    public ArrayList<RolDto> obtenerRolesPorUsuario(long documento) {
        return rlUsuDao.obtenerRolesParaUsuario(documento, miConexion);
    }

    public List obtenerPermisosPorRol(int idRol) {
        return perDao.obtenerPermisosPorRol(idRol, miConexion);
    }

    //Consultas Por Un Criterio (Ciudades y Departamentos)
    public List obtenerTodosDepartamentos() {
        return depDao.obtenerDepartamentos(miConexion);
    }

    public List obtenerCiudadesPorDepartamento(int idDepartamento) {
        return ciDao.obtenerCiudadesPorId(idDepartamento, miConexion);
    }
}
