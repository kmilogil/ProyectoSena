/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utilidades;

import java.sql.Connection;
//import java.util.ArrayList;
//import modulo.ofertas.FOferta;
//import modulo.ofertas.dto.OfertaDto;
//import modulo.ofertas.dto.ProductoDto;
//import modulo.pedidos.FPedido;
//import modulo.pedidos.dao.CarritoDao;
//import modulo.pedidos.dao.InventarioDao;
//import modulo.pedidos.dto.CarritoDto;
//import modulo.pedidos.dto.InventarioDto;
//import modulo.usuarios.FUsuario;
//import modulo.usuarios.dto.PermisoDto;

/**
 *
 * @author User
 */
public class Verificar {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exceptions {
        Connection cnn = Conexion.getInstance();
        if (cnn != null) {
            System.out.println("Run");
        }
//        FUsuario f = new FUsuario();
//        FOferta fa = new FOferta();
//        FPedido fe = new FPedido();

//        ArrayList<ProductoDto> misProductos;
//        misProductos = (ArrayList<ProductoDto>) fa.obtenerProductosAsociados(1073246453);
//        if (misProductos.size() > 0) {
//            for (ProductoDto p : misProductos) {
//                System.out.println(p);
//            }
//
//        } else {
//            System.out.println("nada");
//        }
//        System.out.println(fa.obtenerNumeroDeProductosAsociadosPorProducto(1073247453));
//        ArrayList<OfertaDto> of = new ArrayList();
//        of = (ArrayList<OfertaDto>) fa.obtenerOfertas();
//        for(OfertaDto a: of){
//            System.out.println(a);
//        }
//        int[] producto = new int[2];
//        producto[0] = 4;
//        producto[1] = 31;
//        
//        ArrayList<Integer> respuesta = (ArrayList<Integer>) fa.validarProductoYaAsociado(1073247453, producto);
//        for (Integer a : respuesta) {
//            System.out.println(a);
//        }
//        InventarioDao in = new InventarioDao();
//        InventarioDto aw = new InventarioDto();
//        aw.setIdOferta(13);
//        aw.setCantidad(2);
//
//        InventarioDto aws = new InventarioDto();
//        aws.setIdOferta(14);
//        aws.setCantidad(2);
//
//        InventarioDto awg = new InventarioDto();
//        awg.setIdOferta(15);
//        awg.setCantidad(2);
//        InventarioDto awh = new InventarioDto();
//        awh.setIdOferta(16);
//        awh.setCantidad(2);
//        ArrayList<InventarioDto> ain = new ArrayList();
//        ain.add(aw);
//        ain.add(aws);
//        ain.add(awg);
//        ArrayList<OfertaDto> a = (ArrayList<OfertaDto>) fa.obtenerOfertas(2);
//        for(OfertaDto u:a){
//            System.out.println(u);
//        }
//        CarritoDao ca = new CarritoDao();
//        ArrayList<CarritoDto> caa = (ArrayList<CarritoDto>) ca.obtenerCarrito(2, cnn);
//        String[] salida = null;
//        for (int i = 0; i < caa.size(); i++) {
//            if (caa.get(i).getCantidad() > caa.get(i).getOfDto().getInDto().getCantidad()) {
//                salida = new String[caa.size()];
//                salida[0] = "no";
//                break;
//
//            } else if (caa.get(i).getCantidad() < caa.get(i).getOfDto().getInDto().getCantidad()) {
//                salida = new String[caa.size()];
//                salida[0] = "si";
//            }
//        }
//        String v = "";
//        for (String salida1 : salida) {
//            if (salida1 != null) {
//                v = salida1;
//            }
//        }
//        if (v.equals("no")) {
//            System.out.println("no se puede");
//        } else if (v.equals("si")) {
//            String salida4 = in.descontarInventario(caa, cnn);
//            System.out.println(salida4);
//        }

//            } else if (caa.get(i).getCantidad() < caa.get(i).getOfDto().getInDto().getCantidad()) {
//                System.out.println("no");
//            }
//        }
//        if () {
//            
//        }
//        String salida = in.descontarInventario(caa, cnn);
//                System.out.println(salida);
//        ArrayList<OfertaDto> misOFertas = (ArrayList<OfertaDto>) fa.obtenerOfertasPorProductor(1073247453);
//        if (!misOFertas.isEmpty()) {
//            for (OfertaDto ofertas : misOFertas) {
//                System.out.println(ofertas);
//            }
//
//        }else{
//            System.out.println("h");
//        }
        
//        ArrayList<CarritoDto> c = (ArrayList<CarritoDto>) fe.obtenerDetallePedidosCliente(44);
//        for(CarritoDto a:c){
//            System.out.println(a);
//        }
//        Array fa.obtenerRankingProductor(idUsuario);
    }
}
