package modulo.usuarios.dto;

/**
 * @author Kennit David Ruz Romero Hora de creación: 11:18 a.m. Fecha: 4 de
 * Marzo de 2015
 */
public class UsuarioDto {

    private long idUsuario = 0;
    private String nombres = "";
    private String apellidos = "";
    private String clave = "";
    private String correo = "";
    private String fechaNacimiento = "";
    private String direccion = "";
    private int idCiudad = 0;
    private String fechaSistema = "";
    private String imagen = "";
    private int estado = 0;
    private CiudadDto ciDto = new CiudadDto();

    @Override
    public String toString() {
        return "UsuarioDto{" + "idUsuario=" + idUsuario + ", nombres=" + nombres + ", apellidos=" + apellidos + ", clave=" + clave + ", correo=" + correo + ", fechaNacimiento=" + fechaNacimiento + ", direccion=" + direccion + ", idCiudad=" + idCiudad + ", fechaSistema=" + fechaSistema + ", imagen=" + imagen + ", estado=" + estado + ", ciDto=" + getCiDto() + '}';
    }

    /**
     * @return the idUsuario
     */
    public long getIdUsuario() {
        return idUsuario;
    }

    /**
     * @param idUsuario the idUsuario to set
     */
    public void setIdUsuario(long idUsuario) {
        this.idUsuario = idUsuario;
    }

    /**
     * @return the nombres
     */
    public String getNombres() {
        return nombres;
    }

    /**
     * @param nombres the nombres to set
     */
    public void setNombres(String nombres) {
        this.nombres = nombres;
    }

    /**
     * @return the apellidos
     */
    public String getApellidos() {
        return apellidos;
    }

    /**
     * @param apellidos the apellidos to set
     */
    public void setApellidos(String apellidos) {
        this.apellidos = apellidos;
    }

    /**
     * @return the clave
     */
    public String getClave() {
        return clave;
    }

    /**
     * @param clave the clave to set
     */
    public void setClave(String clave) {
        this.clave = clave;
    }

    /**
     * @return the correo
     */
    public String getCorreo() {
        return correo;
    }

    /**
     * @param correo the correo to set
     */
    public void setCorreo(String correo) {
        this.correo = correo;
    }

    /**
     * @return the fechaNac
     */
    public String getFechaNacimiento() {
        return fechaNacimiento;
    }

    /**
     * @param fechaNac the fechaNac to set
     */
    public void setFechaNacimiento(String fechaNac) {
        this.fechaNacimiento = fechaNac;
    }

    /**
     * @return the direccion
     */
    public String getDireccion() {
        return direccion;
    }

    /**
     * @param direccion the direccion to set
     */
    public void setDireccion(String direccion) {
        this.direccion = direccion;
    }

    /**
     * @return the idCiudad
     */
    public int getIdCiudad() {
        return idCiudad;
    }

    /**
     * @param idCiudad the idCiudad to set
     */
    public void setIdCiudad(int idCiudad) {
        this.idCiudad = idCiudad;
    }

    /**
     * @return the fechaSistema
     */
    public String getFechaSistema() {
        return fechaSistema;
    }

    /**
     * @param fechaSistema the fechaSistema to set
     */
    public void setFechaSistema(String fechaSistema) {
        this.fechaSistema = fechaSistema;
    }

    /**
     * @return the estado
     */
    public int getEstado() {
        return estado;
    }

    /**
     * @param estado the estado to set
     */
    public void setEstado(int estado) {
        this.estado = estado;
    }

    /**
     * @return the imagen
     */
    public String getImagen() {
        return imagen;
    }

    /**
     * @param imagen the imagen to set
     */
    public void setImagen(String imagen) {
        this.imagen = imagen;
    }

    /**
     * @return the ciDto
     */
    public CiudadDto getCiDto() {
        return ciDto;
    }

    /**
     * @param ciDto the ciDto to set
     */
    public void setCiDto(CiudadDto ciDto) {
        this.ciDto = ciDto;
    }
}
