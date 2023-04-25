package lsi.ubu.solucion;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.enunciado.GestionMedicosException;
import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;

/**
 * GestionMedicos: Implementa la gestion de medicos segun el PDF de la carpeta
 * enunciado
 * 
 * @author <a href="mailto:ecl1009@alu.ubu.es">Eduardo Manuel Cabeza Lopez</a>
 * @version 1.0
 * @since 1.0
 */
public class GestionMedicos {

	private static Logger logger = LoggerFactory.getLogger(GestionMedicos.class);

	private static final String script_path = "sql/";
	private static PreparedStatement pst_sel_cliente = null;
	private static PreparedStatement pst_sel_medico = null;
	private static PreparedStatement pst_sel_consulta = null;
	private static PreparedStatement pst_sel_anulacion = null;
	private static PreparedStatement pst_ins_consulta = null;
	private static PreparedStatement pst_upd_medico = null;
	private static PreparedStatement pst_ins_anulacion = null;

	public static void main(String[] args) throws SQLException {
		tests();

		System.out.println("FIN.............");
	}

	public static void reservar_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta)
			throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;

		ResultSet rs_sel_cliente = null;
		ResultSet rs_sel_medico = null;
		ResultSet rs_sel_consulta = null;
		ResultSet rs_sel_anulacion = null;

		try {
			con = pool.getConnection();
			if (pst_sel_cliente == null) {
				pst_sel_cliente = con.prepareStatement("select NIF " + "from cliente " + "where NIF=?");
			}
			pst_sel_cliente.setString(1, m_NIF_cliente);
			rs_sel_cliente = pst_sel_cliente.executeQuery();
			if (!rs_sel_cliente.next()) {
				throw new GestionMedicosException(1);
			}

			if (pst_sel_medico == null) {
				pst_sel_medico = con.prepareStatement("select NIF " + "from medico " + "where NIF=?");
			}
			pst_sel_medico.setString(1, m_NIF_medico);
			rs_sel_medico = pst_sel_medico.executeQuery();
			if (!rs_sel_medico.next()) {
				throw new GestionMedicosException(2);
			}
			// Un médico solo puede tener una consulta en un mismo día.
			// La siguiente sentencia preparada devolverá a lo sumo una fila.
			if (pst_sel_consulta == null) {
				pst_sel_consulta = con.prepareStatement("select id_consulta, fecha_consulta, id_medico, NIF " + "from "
						+ "(select * " + "from consulta join medico " + "on NIF = ? "
						+ "and medico.id_medico = consulta.id_medico "
						+ "and fecha_consulta like ?) c order by fecha_consulta");
			}
			java.sql.Date m_Fecha_sql = new java.sql.Date(m_Fecha_Consulta.getTime());
			pst_sel_consulta.setDate(2, m_Fecha_sql);
			pst_sel_consulta.setString(1, m_NIF_medico);
			rs_sel_consulta = pst_sel_consulta.executeQuery();
			if (rs_sel_consulta.next()) {
				// Por si existe anulación de la consulta
				if (pst_sel_anulacion == null) {
					pst_sel_anulacion = con
							.prepareStatement("select id_anulacion " + "from anulacion " + "where id_consulta = ?");
				}
				rs_sel_consulta.first(); // Asegurar que apuntamos a la primera y única fila.
				pst_sel_anulacion.setInt(1, rs_sel_consulta.getInt(1));
				rs_sel_anulacion = pst_sel_anulacion.executeQuery();
				if (!rs_sel_anulacion.next()) {
					throw new GestionMedicosException(3);
				}
			}
			if (pst_ins_consulta == null) {
				pst_ins_consulta = con
						.prepareStatement("insert into consulta " + "values (seq_consulta.nextval(), ?, ?, ?)");
			}
			pst_ins_consulta.setDate(1, m_Fecha_sql);
			pst_ins_consulta.setInt(2, rs_sel_consulta.getInt(3));
			pst_ins_consulta.setString(3, m_NIF_cliente);
			pst_ins_consulta.executeUpdate();

			if (pst_upd_medico == null) {
				pst_upd_medico = con
						.prepareStatement("update medico" + "set consultas = consultas + ?" + "where NIF = ?");
			}
			pst_upd_medico.setInt(1, 1);
			pst_upd_medico.setString(2, m_NIF_medico);
			pst_upd_medico.executeUpdate();
			con.commit();
		} catch (SQLException e) {
			// Completar por el alumno
			con.rollback();
			logger.error(e.getMessage());
			throw e;

		} finally {
			/* Se liberan todos los recursos que sean necesarios */
			if (rs_sel_cliente != null)
				rs_sel_cliente.close();
			if (rs_sel_medico != null)
				rs_sel_medico.close();
			if (rs_sel_consulta != null)
				rs_sel_consulta.close();
			if (rs_sel_anulacion != null)
				rs_sel_anulacion.close();
			if (pst_sel_cliente != null)
				pst_sel_cliente.close();
			if (pst_sel_medico != null)
				pst_sel_medico.close();
			if (pst_sel_consulta != null)
				pst_sel_consulta.close();
			if (pst_sel_anulacion != null)
				pst_sel_anulacion.close();
			if (pst_ins_consulta != null)
				pst_ins_consulta.close();
			if (pst_upd_medico != null)
				pst_upd_medico.close();
			if (con != null)
				con.close();
		}

	}

	public static void anular_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta,
			Date m_Fecha_Anulacion, String motivo) throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		ResultSet rs_sel_cliente = null;
		ResultSet rs_sel_medico = null;
		ResultSet rs_sel_consulta = null;
		ResultSet rs_sel_anulacion = null;

		long diferenciaEnMilisegundos;
		long diferenciaEnDias;

		try {
			con = pool.getConnection();

			if (pst_sel_cliente == null) {
				pst_sel_cliente = con.prepareStatement("select NIF " + "from cliente " + "where NIF=?");
			}
			pst_sel_cliente.setString(1, m_NIF_cliente);
			rs_sel_cliente = pst_sel_cliente.executeQuery();
			if (!rs_sel_cliente.next()) {
				throw new GestionMedicosException(1);
			}

			if (pst_sel_medico == null) {
				pst_sel_medico = con.prepareStatement("select NIF " + "from medico " + "where NIF=?");
			}
			pst_sel_medico.setString(1, m_NIF_medico);
			rs_sel_medico = pst_sel_medico.executeQuery();
			if (!rs_sel_medico.next()) {
				throw new GestionMedicosException(2);
			}

			if (pst_sel_consulta == null) {
				pst_sel_consulta = con
						.prepareStatement("select id_consulta, fecha_consulta, id_medico, NIF from (select * "
								+ "from consulta join medico " + "on NIF = ? "
								+ "and medico.id_medico = consulta.id_medico "
								+ "and fecha_consulta like ?) c order by fecha_consulta");
			}
			java.sql.Date m_Fecha_sql = new java.sql.Date(m_Fecha_Consulta.getTime());
			pst_sel_consulta.setDate(2, m_Fecha_sql);
			pst_sel_consulta.setString(1, m_NIF_medico);
			rs_sel_consulta = pst_sel_consulta.executeQuery();

			if (!rs_sel_consulta.next()) {
				throw new GestionMedicosException(4);
			} else {
				if (pst_sel_anulacion == null) {
					pst_sel_anulacion = con
							.prepareStatement("select id_anulacion " + "from anulacion " + "where id_consulta = ?");
				}
				rs_sel_consulta.first();
				pst_sel_anulacion.setInt(1, rs_sel_consulta.getInt(1));
				rs_sel_anulacion = pst_sel_anulacion.executeQuery();
				if (rs_sel_anulacion.first()) { // Si es true existe anulacion para la consulta y no se puede volver a
												// anular
					throw new GestionMedicosException(5);
				}
			}

			diferenciaEnMilisegundos = m_Fecha_Consulta.getTime() - m_Fecha_Anulacion.getTime();
			diferenciaEnDias = TimeUnit.DAYS.convert(diferenciaEnMilisegundos, TimeUnit.MILLISECONDS);
			if (diferenciaEnDias < 2) {
				throw new GestionMedicosException(5);
			}

			if (pst_ins_anulacion == null) {
				pst_ins_anulacion = con
						.prepareStatement("insert into anulacion values (seq_anulacion.nextval, ?, ?, ?)");
			}
			java.sql.Date m_Fecha_Anulacion_sql = new java.sql.Date(m_Fecha_Anulacion.getTime());
			pst_ins_anulacion.setInt(1, rs_sel_consulta.getInt(1));
			pst_ins_anulacion.setDate(2, m_Fecha_Anulacion_sql);
			pst_ins_anulacion.setString(3, motivo);
			pst_ins_anulacion.executeUpdate();

			if (pst_upd_medico == null) {
				pst_upd_medico = con
						.prepareStatement("update medico" + "set consultas = consultas + ?" + "where NIF = ?");
			}
			pst_upd_medico.setInt(1, -1);
			pst_upd_medico.setString(2, m_NIF_medico);
			pst_upd_medico.executeUpdate();
			con.commit();

		} catch (SQLException e) {
			// Completar por el alumno
			con.rollback();
			logger.error(e.getMessage());
			throw e;

		} finally {
			/* A rellenar por el alumno, liberar recursos */
			if (rs_sel_cliente != null)
				rs_sel_cliente.close();
			if (rs_sel_medico != null)
				rs_sel_medico.close();
			if (rs_sel_consulta != null)
				rs_sel_consulta.close();
			if (rs_sel_anulacion != null)
				rs_sel_anulacion.close();
			if (pst_sel_cliente != null)
				pst_sel_cliente.close();
			if (pst_sel_medico != null)
				pst_sel_medico.close();
			if (pst_sel_consulta != null)
				pst_sel_consulta.close();
			if (pst_sel_anulacion != null)
				pst_sel_anulacion.close();
			if (pst_ins_anulacion != null)
				pst_ins_anulacion.close();
			if (pst_upd_medico != null)
				pst_upd_medico.close();
			if (con != null)
				con.close();
		}
	}

	public static void consulta_medico(String m_NIF_medico) throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		java.util.Date fecha;
		int consulta;
		int medico;
		String paciente;
		ResultSet rs_sel_consulta = null;
		ResultSet rs_sel_medico = null;
		try {
			con = pool.getConnection();

			if (pst_sel_medico == null) {
				pst_sel_medico = con.prepareStatement("select NIF " + "from medico " + "where NIF=?");
			}
			pst_sel_medico.setString(1, m_NIF_medico);
			rs_sel_medico = pst_sel_medico.executeQuery();
			if (!rs_sel_medico.next()) {
				throw new GestionMedicosException(2);
			}

			if (pst_sel_consulta == null) {
				pst_sel_consulta = con
						.prepareStatement("select id_consulta, fecha_consulta, id_medico, NIF from (select * "
								+ "from consulta join medico " + "on NIF = ? "
								+ "and medico.id_medico = consulta.id_medico "
								+ "and fecha_consulta like ?) c order by fecha_consulta");
			}

			pst_sel_consulta.setString(2, "%");
			pst_sel_consulta.setString(1, m_NIF_medico);
			rs_sel_consulta = pst_sel_consulta.executeQuery();
			medico = rs_sel_consulta.getInt(3);
			System.out.println("Consultas para el médico " + medico);
			while (rs_sel_consulta.next()) {
				fecha = rs_sel_consulta.getDate(2);
				consulta = rs_sel_consulta.getInt(1);
				paciente = new String(rs_sel_consulta.getString(4));
				System.out.println("Fecha: " + fecha + "  Consulta: " + consulta + "NIF Paciente: " + paciente);
			}
			// Commit? rollback? nada?
		} catch (SQLException e) {
			// Completar por el alumno
			con.rollback();
			logger.error(e.getMessage());
			throw e;

		} finally {
			/* A rellenar por el alumno, liberar recursos */
			if (pst_sel_medico != null)
				pst_sel_medico.close();
			if (pst_sel_consulta != null)
				pst_sel_consulta.close();
			if (rs_sel_medico != null)
				rs_sel_medico.close();
			if (rs_sel_consulta != null)
				rs_sel_consulta.close();
			if (con != null)
				con.close();
		}
	}

	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_medicos.sql");
	}

	static void tests() throws SQLException {
		creaTablas();

		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		// Relatar caso por caso utilizando el siguiente procedure para inicializar los
		// datos

		CallableStatement cll_reinicia = null;
		Connection conn = null;

		try {
			// Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();

		}

	}
}
